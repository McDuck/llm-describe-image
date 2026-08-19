# Windows DirectML recognition worker

On the Windows GPU host, create an isolated virtual environment and install only this directory's dependencies:

```powershell
cd external_gpu_host\recognition\windows
py -m venv .venv
.\.venv\Scripts\python -m pip install -r requirements.txt
$env:RECOGNITION_API_TOKEN = "replace-with-a-long-random-secret"
.\run.ps1 -Python .\.venv\Scripts\python.exe
```

`run.ps1` requires `RECOGNITION_API_TOKEN` or `-Token`, forces `DmlExecutionProvider`, and defaults to `127.0.0.1:5002`. It accepts `-Host`, `-Port`, `-Model`, and `-Token` overrides. It runs the neighbouring standalone worker and does not install Describe Media.

For a remote pipeline host, keep the worker running and open this reverse SSH tunnel in another PowerShell window:

```powershell
ssh -N -o ExitOnForwardFailure=yes -o ServerAliveInterval=30 -o ServerAliveCountMax=3 `
  -R 127.0.0.1:15003:127.0.0.1:5002 deploy@pipeline-host.example.invalid
```

Configure `RECOGNITION_API_BASE=http://127.0.0.1:15003/v1`, the same `RECOGNITION_API_TOKEN`, and `RECOGNITION_API_TIMEOUT_S=120` on the pipeline host. Both endpoints remain loopback-only.
