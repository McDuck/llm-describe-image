# Describe Media recognition

This package owns the Describe Media recognition client, local InsightFace backend, and reviewed identity index. Its backends mirror video extraction: `gpus/api/` calls the shared GPU API, while `gpus/direct/` runs InsightFace on the pipeline host. They are not shared with the remote worker, which has its own deployment-local implementation.

The normal `describe` pipeline recognises faces in the original image before LLM inference. Only reviewed identities from `OUTPUT_DIR/recognition/model-manifest.json` are included in the LLM context. `recognition-cluster` produces provisional `cluster-*` folders for the review app, and `recognition-train` turns renamed identity folders into the index.

Install local inference with `pip install -e "describe_media[recognition]"`. To delegate inference, retain the existing environment contract:

```dotenv
GPU_API_BASE=http://127.0.0.1:15003/v1
GPU_API_TOKEN=replace-with-a-long-random-secret
GPU_API_TIMEOUT_S=120
```

`RemoteRecognitionBackend` preserves a 120-second default timeout, sends bearer authentication, and calls `GET /v1/health` plus `POST /v1/recognition`. The same `GPU_API_*` configuration also enables remote video-frame extraction. Deploy the remote side from [`../../external_gpu_host/gpu/README.md`](../../external_gpu_host/gpu/README.md).
