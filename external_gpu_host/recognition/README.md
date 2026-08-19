# Remote recognition worker

The worker owns remote InsightFace inference. Describe Media remains responsible for clustering, matching, review artifacts, and index training. The HTTP API is fixed: authenticated `GET /v1/health` and `POST /v1/recognition` only.

## Generic local worker

Install only the worker requirements, then run its explicit entry point:

```powershell
cd external_gpu_host/recognition/worker
py -m venv .venv
.\.venv\Scripts\python -m pip install -r requirements.txt
$env:RECOGNITION_API_TOKEN = "replace-with-a-long-random-secret"
.\.venv\Scripts\python server.py --host 127.0.0.1 --port 5002
```

The worker defaults to loopback `127.0.0.1:5002`, requires the bearer token on every request, uses the configured `RECOGNITION_MODEL` or `buffalo_l`, and serialises inference to protect DirectML sessions and VRAM.

## Windows DirectML worker and reverse tunnel

Use [`windows/README.md`](windows/README.md) for a Windows AMD GPU. Keep the worker loopback-only and publish it to a Linux/remote Describe Media host through a reverse tunnel:

```powershell
ssh -N -o ExitOnForwardFailure=yes -o ServerAliveInterval=30 -o ServerAliveCountMax=3 `
  -R 127.0.0.1:15003:127.0.0.1:5002 deploy@pipeline-host.example.invalid
```

On the pipeline host, configure the existing client variables with the exact same token:

```dotenv
RECOGNITION_API_BASE=http://127.0.0.1:15003/v1
RECOGNITION_API_TOKEN=replace-with-a-long-random-secret
RECOGNITION_API_TIMEOUT_S=120
```

Verify from that host with an authenticated request to `http://127.0.0.1:15003/v1/health`. Do not bind either the worker or tunnel listener to a LAN interface.
