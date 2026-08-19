# Windows KoboldCpp with Describe Media

Run KoboldCpp with its OpenAI-compatible API on local port `5001`. The Linux Describe Media host reaches it through a loopback-only reverse tunnel:

```dotenv
BACKEND=openai
MODEL_NAME=qwen/qwen3-vl-8b
OPENAI_API_BASE=http://127.0.0.1:15002/v1
OPENAI_API_KEY=
```

Run `ssh -N -R 127.0.0.1:15002:127.0.0.1:5001 deploy@pipeline-host` from the Windows GPU host, then set the values above in the Linux host's `describe_media/config/.env`.
