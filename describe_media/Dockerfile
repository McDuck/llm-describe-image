FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc libgl1 libglib2.0-0 libgomp1 \
    && rm -rf /var/lib/apt/lists/*

COPY describe_media/pyproject.toml ./describe_media/pyproject.toml
COPY describe_media ./describe_media

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir \
        lmstudio Pillow geopy python-dotenv pytest \
        numpy==1.26.4 opencv-python-headless==4.10.0.84 \
        insightface onnxruntime \
    && pip install --no-cache-dir --no-deps -e ./describe_media

# Cache the recognition model in the image so startup does not download it.
RUN python -c "from insightface.app import FaceAnalysis; FaceAnalysis(name='buffalo_l', providers=['CPUExecutionProvider'])"

ENTRYPOINT ["python", "-m", "describe_media.describe_media"]
CMD ["all"]
