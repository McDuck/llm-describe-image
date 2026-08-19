# Recognition tasks

`RecognitionPreparationTask` writes review artifacts for `recognition-cluster`. `RecognitionTask` runs in the normal describe graph, matches only the trained, reviewed index, and writes per-image cut-out manifests and crops. Matched candidates may be copied to provisional review clusters; set `RECOGNITION_COPY_MATCHES_TO_REVIEW_CLUSTERS=false` to opt out.

The tasks preserve source-relative paths and use original, EXIF-orientation-corrected image pixels for recognition. See [`../../recognition/README.md`](../../recognition/README.md) for configuration.
