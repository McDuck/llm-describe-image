# Recognition training pipeline

`recognition-train` builds `OUTPUT_DIR/recognition/model-manifest.json` from reviewer-approved identity folders. Provisional folders whose names start with `cluster-` are excluded. The command also creates an inspectable `model/identity.json` below every reviewed identity.

Run `describe-media recognition-train <input-dir> <output-dir>`. It uses the local InsightFace runtime unless `RECOGNITION_API_BASE`, `RECOGNITION_API_TOKEN`, and optionally `RECOGNITION_API_TIMEOUT_S` direct it to the authenticated worker. Do not run training concurrently with another process that writes the same recognition output tree.
