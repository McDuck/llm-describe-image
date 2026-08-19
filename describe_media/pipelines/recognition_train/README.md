# Recognition training pipeline

`recognition-train` builds `OUTPUT_DIR/recognition/model-manifest.json` from reviewer-approved identity folders. Provisional folders whose names start with `cluster-` are excluded. The command also creates an inspectable `model/identity.json` below every reviewed identity.

Run `describe-media recognition-train <input-dir> <output-dir>`. It uses the local InsightFace runtime unless `GPU_API_BASE`, `GPU_API_TOKEN`, and optionally `GPU_API_TIMEOUT_S` direct it to the authenticated shared GPU API. Do not run training concurrently with another process that writes the same recognition output tree.
