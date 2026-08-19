# Describe Media recognition

This package owns the Describe Media recognition client, local InsightFace backend, and reviewed identity index. It is not shared with the remote worker; the worker has its own deployment-local backend.

The normal `describe` pipeline recognises faces in the original image before LLM inference. Only reviewed identities from `OUTPUT_DIR/recognition/model-manifest.json` are included in the LLM context. `recognition-cluster` produces provisional `cluster-*` folders for the review app, and `recognition-train` turns renamed identity folders into the index.

Install local inference with `pip install -e "describe_media[recognition]"`. To delegate inference, retain the existing environment contract:

```dotenv
RECOGNITION_API_BASE=http://127.0.0.1:15003/v1
RECOGNITION_API_TOKEN=replace-with-a-long-random-secret
RECOGNITION_API_TIMEOUT_S=120
```

`RemoteRecognitionBackend` preserves a 120-second default timeout, sends bearer authentication, and calls only `GET /v1/health` and `POST /v1/recognition`. Deploy the remote side from [`../../external_gpu_host/recognition/README.md`](../../external_gpu_host/recognition/README.md).
