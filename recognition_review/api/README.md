# Recognition Review API

The API is a filesystem-backed service that lists provisional clusters and moves their JSON, crop, and shortcut artifacts into reviewed identity folders. It deliberately has no database or ML runtime.

`RECOGNITION_ROOT` defaults to `/data/output/recognition`; `REVIEW_PORT` defaults to `8081`. The Docker image runs `server.py` and is composed by the parent application.
