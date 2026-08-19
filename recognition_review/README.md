# Recognition Review

Recognition Review is a separate application for accepting, renaming, splitting, or rejecting provisional `OUTPUT_DIR/recognition/cluster-*` folders. It has no InsightFace dependency and works directly on the generated JSON and crop files.

Run it from the repository root:

```powershell
Copy-Item recognition_review\config\.env.example recognition_review\config\.env
docker compose --env-file recognition_review/config/.env -f recognition_review/docker-compose.yml up --build
```

Set `OUTPUT_DIR` to the Describe Media output directory, then open `http://localhost:8080`. After review, run Describe Media `recognition-train` to build the identity index. The API and UI details are documented in their component READMEs.
