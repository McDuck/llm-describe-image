from describe_media.pipelines.recognition_train import RecognitionTrainingPipeline


def test_training_pipeline_uses_remote_worker_when_configured(tmp_path, monkeypatch) -> None:
    input_root = tmp_path / "input"
    output_root = tmp_path / "output"
    captured = {}

    def fake_train(**kwargs):
        captured.update(kwargs)
        return output_root / "recognition" / "model-manifest.json"

    monkeypatch.setenv("RECOGNITION_API_BASE", "http://127.0.0.1:15003/v1")
    monkeypatch.setenv("RECOGNITION_API_TOKEN", "test-token")
    monkeypatch.setenv("RECOGNITION_API_TIMEOUT_S", "30")
    monkeypatch.setattr("describe_media.pipelines.recognition_train.pipeline.train_recognition_index", fake_train)

    RecognitionTrainingPipeline().run(str(input_root), str(output_root))

    backend = captured["backend"]
    assert backend.api_base == "http://127.0.0.1:15003/v1"
    assert backend.token == "test-token"
    assert backend.timeout_seconds == 30.0
