# Describe Media application

Describe Media discovers images and video frames, collects metadata, runs reviewer-curated face recognition, asks an LLM for descriptions, and can refine those descriptions using nearby media as context. Its CLI, Docker services, local recognition code, remote worker assets, and operational guidance all live in this directory.

## Run

From the repository root, copy `describe_media/.env.example` to `describe_media/.env`, set `INPUT_DIR` and `OUTPUT_DIR`, then choose either local Python or Docker. The example targets a local OpenAI-compatible endpoint; set `OPENAI_API_BASE=https://api.openai.com/v1` and `OPENAI_API_KEY` if you prefer the hosted OpenAI API:

```powershell
python -m describe_media.describe_media describe <input-dir> <output-dir>
docker compose --env-file describe_media/.env -f describe_media/docker-compose.yml up --build describe_media
```

The normal `describe` graph discovers media, creates video frames, persists metadata, resizes images for the LLM, runs recognition on the original image, calls the LLM, and adds context enhancement. Metadata, resize, and recognition are complete before captioning; reverse geocoding is optional and never holds up captioning. A failed caption produces an `.error.txt`; use `--retry-failed` to retry those items without reprocessing successful ones.

`enhance` is a targeted pass that reads existing captions, gathers nearby same-directory/date descriptions, and writes `<image>.enhanced.txt`. Tune its model, prompt, context window, and item limits in `config/config.defaults.toml`. The `huggingface` backend supports local transformer inference (including automatic device selection and optional quantization); select it with `BACKEND=huggingface` and configure the `[huggingface]` settings in that file.

The task engine uses bounded queues, configurable worker counts, backpressure, and orderly Ctrl+C shutdown. Define a new pipeline in `pipelines/`, register it in `pipelines/__init__.py`, and add individual stages below `tasks/`.

## Recognition

The normal `describe` workflow matches only identities that a human has reviewed. `recognition-cluster` creates provisional `cluster-*` review folders and `recognition-train` creates `OUTPUT_DIR/recognition/model-manifest.json` plus inspectable per-identity models. See [`recognition/README.md`](recognition/README.md), the pipeline READMEs, and [`../external_gpu_host/recognition/README.md`](../external_gpu_host/recognition/README.md) for a standalone remote GPU worker.

Install local inference with `pip install -e "describe_media[recognition]"`; install video support with `pip install -e "describe_media[video]"`. The browser reviewer is a separate app in [`../recognition_review/README.md`](../recognition_review/README.md).

## Docker deployment

Compose targets Linux hosts with existing local or network-mounted storage. It uses host networking so loopback-only reverse SSH tunnels remain private and reachable from the application container:

```powershell
docker compose --env-file describe_media/.env -f describe_media/docker-compose.yml up --build describe_media

# Linux-host, index-only recognition training.
docker compose --env-file describe_media/.env -f describe_media/docker-compose.yml up --build recognition-train
```

Compose bind-mounts the existing paths from `.env`; it does not stage data. To use a different container environment file, set `DESCRIBE_MEDIA_ENV_FILE` to its path relative to this Compose file. `scripts/release-remote-host.ps1` deploys the application to a Linux host and starts the `describe_media` service.

## Windows KoboldCpp

KoboldCpp can replace `api.openai.com` while retaining the `openai` backend contract. When KoboldCpp runs on a Windows GPU host, publish its local port `5001` to the Linux pipeline host through a loopback-only reverse tunnel, then set:

```dotenv
BACKEND=openai
MODEL_NAME=qwen/qwen3-vl-8b
OPENAI_API_BASE=http://127.0.0.1:15002/v1
OPENAI_API_KEY=
```

Create the tunnel from Windows: `ssh -N -R 127.0.0.1:15002:127.0.0.1:5001 deploy@pipeline-host`. On the Linux host, run the Compose command above.
