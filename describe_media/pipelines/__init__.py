"""Pipeline loader for orchestrating image processing workflows."""

from typing import Optional, Dict
from describe_media.pipelines.pipeline import Pipeline


def get_pipeline(pipeline_name: str) -> Optional[Pipeline]:
    """
    Get a pipeline by name.
    
    Args:
        pipeline_name: Name of the pipeline (e.g., 'describe', 'enhance', 'geolocate')
    
    Returns:
        Pipeline instance or None if not found.
    """
    if pipeline_name == "describe":
        from describe_media.pipelines.describe import DescribePipeline
        return DescribePipeline()
    elif pipeline_name == "enhance":
        from describe_media.pipelines.enhance import EnhanceByContextPipeline
        return EnhanceByContextPipeline()
    elif pipeline_name == "geolocate":
        from describe_media.pipelines.geolocate import GeolocationPipeline
        return GeolocationPipeline()
    elif pipeline_name == "recognition-cluster":
        from describe_media.pipelines.recognition_cluster import RecognitionClusterPipeline
        return RecognitionClusterPipeline()
    elif pipeline_name == "recognition-train":
        from describe_media.pipelines.recognition_train import RecognitionTrainingPipeline
        return RecognitionTrainingPipeline()

    return None


def list_pipelines() -> Dict[str, str]:
    """
    List all available pipelines.
    
    Returns:
        Dictionary mapping pipeline names to descriptions
    """
    return {
        "describe": "Runs the integrated media description graph",
        "enhance": "Enhances existing descriptions using context from nearby images",
        "geolocate": "Reverse geocodes GPS coordinates to human-readable locations (Discover â†’ SkipCheck â†’ Geolocate â†’ Write)",
        "recognition-cluster": "Creates face-recognition clusters for later review (Discover, Download, Recognize)",
        "recognition-train": "Builds a local identity index from reviewed recognition folders",
    }
