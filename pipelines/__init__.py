"""Pipeline loader for orchestrating image processing workflows."""

from typing import Optional, Dict
from pipelines.pipeline import Pipeline


def get_pipeline(pipeline_name: str) -> Optional[Pipeline]:
    """
    Get a pipeline by name.
    
    Args:
        pipeline_name: Name of the pipeline (e.g., 'describe-image')
    
    Returns:
        Pipeline instance or None if not found
    """
    if pipeline_name == "describe-image":
        from pipelines.describe_image import DescribeImagePipeline
        return DescribeImagePipeline()
    
    return None


def list_pipelines() -> Dict[str, str]:
    """
    List all available pipelines.
    
    Returns:
        Dictionary mapping pipeline names to descriptions
    """
    return {
        "describe-image": "Describes images using LLM (Discover → SkipCheck → Download → LLM → Write)",
    }
