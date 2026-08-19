# Remote video-frame extraction

Video-frame extraction is maintained separately from the face-recognition implementation in this directory. It is exposed through the shared GPU API listener as authenticated `POST /v1/video-frames`; the listener is at [`../../server.py`](../../server.py), so recognition and frame extraction share one `/v1` base URL and one `GPU_API_TOKEN`.

The shared worker requirements include the OpenCV dependency required here. Configure the Describe Media pipeline once:

```dotenv
GPU_API_BASE=http://127.0.0.1:15003/v1
GPU_API_TOKEN=replace-with-a-long-random-secret
GPU_API_TIMEOUT_S=120
```

The pipeline streams each source video to the worker and writes returned JPEGs plus the normal extraction manifest locally. See [`../../README.md`](../../README.md) for worker startup and tunnel instructions.
