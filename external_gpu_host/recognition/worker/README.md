# Generic recognition worker

`server.py` is a standalone Python entry point. Install `requirements.txt`; it has no dependency on the Describe Media package. Start it with `python server.py --host 127.0.0.1 --port 5002 --token <token>`.

Its default provider is `DmlExecutionProvider` for compatibility with the Windows launcher; on hosts without DirectML, pass `--provider CPUExecutionProvider`. It protects all inference calls with a single lock and never changes the API paths or bearer-token requirement.
