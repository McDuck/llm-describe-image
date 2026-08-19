# Remote recognition worker

The shared GPU API owns remote InsightFace inference and optional video-frame decoding. The two services live below `services/`: face recognition in [`services/recognition/`](services/recognition/) and frame extraction in [`services/video_frames/`](services/video_frames/). Describe Media remains responsible for clustering, matching, review artifacts, index training, and writing frame files. Its authenticated API is `GET /v1/health`, `POST /v1/recognition`, and `POST /v1/video-frames`.

## Linux local worker

For a source checkout, begin at the repository root. For the `external_gpu_host.tar.gz` archive created by `release.sh`, extract it first; it preserves the `external_gpu_host/` directory. Copy [`.env.example`](.env.example) to `.env`, set `GPU_API_TOKEN`, then install the worker requirements and run its explicit entry point. The server loads `gpu/.env` automatically:

```powershell
tar -xzf external_gpu_host.tar.gz  # Omit when running from the repository checkout.
cd external_gpu_host\gpu
Copy-Item .env.example .env
# Edit .env and replace GPU_API_TOKEN.
py -m venv .venv
.\.venv\Scripts\python -m pip install -r os\linux\requirements.txt
.\.venv\Scripts\python server.py
```

The `.env` file is the preferred source for `GPU_API_HOST`, `GPU_API_PORT`, `GPU_API_TOKEN`, model/provider, and the video-upload limit. Command-line arguments are optional overrides. The worker defaults to loopback `127.0.0.1:5002`, requires the bearer token on every request, uses the configured `RECOGNITION_MODEL` or `buffalo_l`, and serialises inference to protect GPU sessions and VRAM. On Windows, install [`os/windows/requirements.txt`](os/windows/requirements.txt) instead, as documented below.

## Windows DirectML worker and reverse tunnel

Use [`os/windows/README.md`](os/windows/README.md) for a Windows AMD GPU. Keep the worker loopback-only and publish it to a Linux/remote Describe Media host through a reverse tunnel:

```powershell
ssh -N -o ExitOnForwardFailure=yes -o ServerAliveInterval=30 -o ServerAliveCountMax=3 `
  -R 127.0.0.1:15003:127.0.0.1:5002 deploy@pipeline-host.example.invalid
```

On the pipeline host, configure the existing client variables with the exact same token:

```dotenv
GPU_API_BASE=http://127.0.0.1:15003/v1
GPU_API_TOKEN=replace-with-a-long-random-secret
GPU_API_TIMEOUT_S=120
```

`POST /v1/video-frames` receives the source video as an authenticated binary upload and responds with sampled JPEGs. The pipeline writes those JPEGs and its normal manifest locally, so all downstream behavior is unchanged. The worker serialises it with recognition to protect GPU/decoder resources. It accepts uploads up to 8 GiB by default; set `VIDEO_FRAME_API_MAX_UPLOAD_BYTES` to lower that ceiling. Verify from that host with an authenticated request to `http://127.0.0.1:15003/v1/health`. Do not bind either the worker or tunnel listener to a LAN interface.
