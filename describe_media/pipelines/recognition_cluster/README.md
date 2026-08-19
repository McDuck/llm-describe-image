# Recognition clustering pipeline

`recognition-cluster` finds faces and writes provisional reviewer records below `OUTPUT_DIR/recognition/cluster-*`. Run it for one input subtree, a UTF-8 manifest of source-relative image paths, or a random sample:

```powershell
describe-media recognition-cluster <input-dir> <output-dir> --subdirectory "2026\2026-07"
describe-media recognition-cluster <input-dir> <output-dir> --random-sample 500
```

Review and rename the clusters in the Recognition Review application before running `recognition-train`. The worker configuration is identical to the recognition package configuration.
