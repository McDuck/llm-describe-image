# Describe Media

Describe Media is a task-based image and video description pipeline with optional reviewer-curated face recognition. This repository is organised by deployable domain rather than by a shared implementation layer.

| Component | Purpose | Start here |
| --- | --- | --- |
| `describe_media/` | CLI, pipelines, local recognition, and application containers | [`describe_media/README.md`](describe_media/README.md) |
| `recognition_review/` | Browser UI and filesystem-backed API for review clusters | [`recognition_review/README.md`](recognition_review/README.md) |
| `external_gpu_host/` | Independently deployed GPU-side services | [`external_gpu_host/README.md`](external_gpu_host/README.md) |

## Quick start

```powershell
py -m venv .venv
.\.venv\Scripts\python -m pip install -e "describe_media[dev]"
Copy-Item describe_media\config\.env.example describe_media\config\.env
describe-media describe <input-dir> <output-dir>
```

Set `OPENAI_API_KEY` and `INPUT_DIR`/`OUTPUT_DIR` in `describe_media/config/.env` when using the defaults. For Docker, run:

```powershell
docker compose --env-file describe_media/config/.env -f describe_media/docker-compose.yml up --build describe_media
```

The normal graph processes discovery, metadata, resize, recognition, LLM inference, and enhancement. Recognition is local by default; see [`describe_media/recognition/README.md`](describe_media/recognition/README.md) to use the authenticated remote worker without changing the API contract.

## Tests

Unit tests live beside their owning components. `describe_media/tests/` contains only cross-domain integration and end-to-end coverage. Run all tests with:

```powershell
python -m pytest -q
```

Run the Docker E2E suite with `docker compose --env-file describe_media/config/.env -f describe_media/docker-compose.yml run --rm e2e`.

## License

MIT
