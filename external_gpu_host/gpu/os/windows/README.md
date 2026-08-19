# Windows DirectML shared GPU API

On the Windows GPU host, create an isolated virtual environment and install only this directory's dependencies:

```powershell
Copy-Item external_gpu_host\gpu\.env.example external_gpu_host\gpu\.env
cd external_gpu_host\gpu
py -m venv .venv
.\.venv\Scripts\python -m pip install -r os\windows\requirements.txt
.\.venv\Scripts\python server.py
```

Set `GPU_API_TOKEN` in `gpu/.env`; `server.py` loads that file automatically and uses its `GPU_API_HOST`/`GPU_API_PORT` values. The Windows requirements select `onnxruntime-directml`, which provides `DmlExecutionProvider`. Pass `--host`, `--port`, `--model`, or `--token` directly to `server.py` when an override is needed.

If this environment was previously installed with the CPU `onnxruntime` package, repair it before starting the server:

```powershell
.\.venv\Scripts\python -m pip uninstall -y onnxruntime onnxruntime-directml
.\.venv\Scripts\python -m pip install --no-cache-dir --force-reinstall onnxruntime-directml
.\.venv\Scripts\python -c "import onnxruntime as ort; print(ort.get_available_providers())"
```

The final command must include `DmlExecutionProvider`; otherwise the server will now stop rather than silently falling back to CPU.

For a remote pipeline host, keep the worker running and open this reverse SSH tunnel in another PowerShell window:

```powershell
ssh -N -o ExitOnForwardFailure=yes -o ServerAliveInterval=30 -o ServerAliveCountMax=3 `
  -R 127.0.0.1:15003:127.0.0.1:5002 deploy@pipeline-host.example.invalid
```

Configure `GPU_API_BASE=http://127.0.0.1:15003/v1`, the same `GPU_API_TOKEN`, and `GPU_API_TIMEOUT_S=120` on the pipeline host. Both endpoints remain loopback-only.
