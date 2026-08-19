from describe_media.pipelines.recognition_cluster import RecognitionClusterPipeline


def test_recognition_preparation_pipeline_uses_remote_worker_when_configured(monkeypatch) -> None:
    monkeypatch.setenv("GPU_API_BASE", "http://gpu-host.example:5002/v1")
    monkeypatch.setenv("GPU_API_TOKEN", "test-token")
    monkeypatch.setenv("GPU_API_TIMEOUT_S", "30")
    pipeline = RecognitionClusterPipeline()
    pipeline.input_dir = "/input"
    pipeline.output_dir = "/output"

    recognition_config = next(item for item in pipeline.PIPELINE_CONFIG if item["name"] == "Recognize")
    options = recognition_config["kwargs_builder"](pipeline)

    assert options["remote_api_base"] == "http://gpu-host.example:5002/v1"
    assert options["remote_api_token"] == "test-token"
    assert options["remote_timeout_seconds"] == 30.0
