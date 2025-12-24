"""Pipeline loader for orchestrating image processing workflows."""

from typing import Optional, Dict
from pipelines.pipeline import Pipeline


def get_pipeline(pipeline_name: str) -> Optional[Pipeline]:
    """
    Get a pipeline by name.
    
    Args:
        pipeline_name: Name of the pipeline (e.g., 'describe', 'enhance', 'geolocate')
    
    Returns:
        Pipeline instance or None if not found
    """
    if pipeline_name == "describe":
        from pipelines.describe import DescribePipeline
        return DescribePipeline()
    elif pipeline_name == "enhance":
        from pipelines.enhance import EnhanceByContextPipeline
        return EnhanceByContextPipeline()
    elif pipeline_name == "geolocate":
        from pipelines.geolocate import GeolocationPipeline
        return GeolocationPipeline()
    elif pipeline_name == "shortcut":
        from pipelines.shortcut import ShortcutPipeline
        return ShortcutPipeline()
    elif pipeline_name == "fix-jpeg":
        from pipelines.fix_jpeg import FixJpegPipeline
        return FixJpegPipeline()
    
    return None


def list_pipelines() -> Dict[str, str]:
    """
    List all available pipelines.
    
    Returns:
        Dictionary mapping pipeline names to descriptions
    """
    return {
        "describe": "Describes images using LLM (Discover → SkipCheck → Download → LLM → Write)",
        "enhance": "Enhances descriptions using context from nearby images (Discover → SkipCheck → Context → Enhance → Write)",
        "geolocate": "Reverse geocodes GPS coordinates to human-readable locations (Discover → SkipCheck → Geolocate → Write)",
        "shortcut": "Creates Windows shortcuts linking to original images (Discover → SkipCheck → Shortcut)",
        "fix-jpeg": "Fixes corrupted JPEG files by re-encoding them (Discover → FixJpeg)",
    }
