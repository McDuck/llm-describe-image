const $ = (selector, element = document) => element.querySelector(selector);

async function request(path, payload) {
  const response = await fetch(path, {method: "POST", headers: {"Content-Type": "application/json"}, body: JSON.stringify(payload)});
  let result;
  try { result = await response.json(); }
  catch { throw new Error(`Review action failed (server returned ${response.status}). Refresh to check whether it completed.`); }
  if (!response.ok) throw new Error(result.error || "Review action failed");
  return result;
}
function clone(template) { return $(template).content.firstElementChild.cloneNode(true); }
function setStatus(message, error = false) { const status = $("#status"); status.textContent = message; status.classList.toggle("error", error); }
function labelInput(input) { const label = input.value.trim(); if (!label) { input.focus(); throw new Error("Enter a label first."); } return label; }
function videoSampleInfo(source, filename) {
  const video = source.video;
  const match = filename.match(/\.frame-(\d+)-t(\d+(?:\.\d+)?)\.jpg$/i);
  const frame = video?.frame_number ?? source.frame_number ?? match?.[1];
  const seconds = video?.timestamp_seconds ?? source.frame_timestamp_seconds ?? (match ? Number(match[2]) : null);
  if (frame == null || seconds == null) return "";
  const totalMilliseconds = Math.round(Number(seconds) * 1000);
  const hours = Math.floor(totalMilliseconds / 3600000);
  const minutes = Math.floor((totalMilliseconds % 3600000) / 60000);
  const remainingSeconds = Math.floor((totalMilliseconds % 60000) / 1000);
  const milliseconds = totalMilliseconds % 1000;
  const timestamp = [hours, minutes, remainingSeconds].map(value => String(value).padStart(2, "0")).join(":") + `.${String(milliseconds).padStart(3, "0")}`;
  return `Frame ${String(frame).padStart(4, "0")} · ${timestamp}`;
}
function action(button, callback) {
  button.addEventListener("click", async () => {
    try { button.disabled = true; const result = await callback(); setStatus(result.message); await load(); }
    catch (error) { setStatus(error.message, true); }
    finally { button.disabled = false; }
  });
}
function render(data) {
  const container = $("#clusters"); container.replaceChildren();
  const labels = $("#labels"); labels.replaceChildren(...data.labels.map(label => Object.assign(document.createElement("option"), {value: label})));
  $("#empty").hidden = data.clusters.length !== 0;
  for (const cluster of data.clusters) {
    const card = clone("#cluster-template");
    $(".cluster-id", card).textContent = cluster.false_positive ? `${cluster.id} · false positives` : cluster.id;
    $(".count", card).textContent = cluster.count;
    const clusterInput = $(".cluster-label", card);
    action($(".label-cluster", card), () => request("/api/clusters/label", {cluster_id: cluster.id, label: labelInput(clusterInput)}));
    action($(".false-positive-cluster", card), () => request("/api/clusters/false-positive", {cluster_id: cluster.id}));
    for (const item of cluster.items) {
      const row = clone("#item-template"); const image = $(".crop", row);
      if (item.crop_url) image.src = item.crop_url;
      else image.replaceWith(Object.assign(document.createElement("div"), {className: "crop missing", textContent: "No crop"}));
      const pathParts = (item.source.relative_path || item.record_path).split("/");
      const dateFolder = pathParts.length > 1 ? pathParts.at(-2) : "";
      const filename = item.source.filename || item.record_path;
      $(".filename", row).textContent = [filename, dateFolder, videoSampleInfo(item.source, filename)].filter(Boolean).join(" · ");
      $(".confidence", row).textContent = item.detection_confidence ? `Detection ${Math.round(item.detection_confidence * 100)}%` : "";
      const itemInput = $(".item-label", row);
      action($(".label-item", row), () => request("/api/items/label", {cluster_id: cluster.id, record_path: item.record_path, label: labelInput(itemInput)}));
      action($(".false-positive-item", row), () => request("/api/items/false-positive", {cluster_id: cluster.id, record_path: item.record_path}));
      $(".items", card).append(row);
    }
    container.append(card);
  }
  setStatus(data.clusters.length ? `${data.clusters.length} unlabelled clusters ready.` : "All clusters are reviewed.");
}
async function load() {
  try { render(await (await fetch("/api/review")).json()); }
  catch (error) { setStatus(`Could not load review data: ${error.message}`, true); }
}
$("#refresh").addEventListener("click", load); load();
