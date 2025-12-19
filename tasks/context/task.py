import os
import sys
import threading
from typing import Optional, Tuple, List, Dict, Any
from datetime import datetime, timedelta
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from tasks.task import Task


class ContextTask(Task[str, Tuple[str, str, List[str]]]):
    """
    Gather context from nearby images for description enhancement.
    
    Input: image path
    Output: (image_path, original_description, context_descriptions)
    """
    
    # Class-level cache shared across all threads
    _shared_images_cache: Optional[List[str]] = None
    _shared_cache_lock: threading.Lock = threading.Lock()
    _cache_initialized: bool = False
    
    def __init__(
        self,
        maximum: int = 1,
        input_dir: Optional[str] = None,
        output_dir: Optional[str] = None,
        context_window_days: int = 10,
        max_context_items: int = 20
    ) -> None:
        super().__init__(maximum, input_dir=input_dir)
        self.output_dir: Optional[str] = output_dir
        self.context_window_days: int = context_window_days
        self.max_context_items: int = max_context_items
        self._file_cache: Dict[str, Tuple[Optional[datetime], str]] = {}  # Cache: path -> (datetime, description)
    
    def load(self) -> None:
        """Pre-load image cache before threads start (once for all threads)."""
        super().load()
        
        # Only the first thread builds the cache
        if not ContextTask._cache_initialized:
            with ContextTask._shared_cache_lock:
                if not ContextTask._cache_initialized:  # Double-check
                    print("Building image cache for context gathering...")
                    ContextTask._shared_images_cache = self._discover_images(self.input_dir)
                    ContextTask._cache_initialized = True
                    print(f"Found {len(ContextTask._shared_images_cache)} images for context")
    
    def execute(self, item: str) -> Tuple[str, str, List[str]]:
        """
        Gather context descriptions from nearby images.
        Args: input_path
        Returns: (input_path, original_description, context_descriptions)
        """
        input_path = item
        
        # Read the original description
        original_desc = self._read_description(input_path)
        if not original_desc:
            raise Exception(f"No description file found for {input_path}")
        
        # Get metadata for target image
        target_metadata = self._get_metadata(input_path)
        target_datetime = target_metadata.get('datetime')
        target_dir = os.path.dirname(input_path)
        target_filename = os.path.basename(input_path)
        
        # Find all image files in input directory (cached)
        all_images = self._get_all_images()
        
        # Score and filter context candidates
        context_candidates: List[Tuple[float, str, str]] = []  # (score, path, description)
        
        for img_path in all_images:
            if img_path == input_path:
                continue  # Skip self
            
            desc = self._read_description(img_path)
            if not desc:
                continue  # Skip images without descriptions
            
            # Calculate relevance score
            score = self._calculate_relevance_score(
                input_path, target_datetime, target_dir, target_filename,
                img_path, self._get_metadata(img_path)
            )
            
            if score > 0:
                context_candidates.append((score, img_path, desc))
        
        # Sort by score (highest first) and take top N
        context_candidates.sort(reverse=True, key=lambda x: x[0])
        context_descriptions = [desc for _, _, desc in context_candidates[:self.max_context_items]]
        
        return (input_path, original_desc, context_descriptions)
    
    def _read_description(self, image_path: str) -> Optional[str]:
        """Read description file for an image."""
        if self.input_dir and self.output_dir:
            relative = os.path.relpath(image_path, self.input_dir)
            desc_file = os.path.join(self.output_dir, relative + ".txt")
        else:
            desc_file = image_path + ".txt"
        
        if not os.path.exists(desc_file):
            return None
        
        try:
            with open(desc_file, "r", encoding="utf-8") as f:
                return f.read().strip()
        except Exception:
            return None
    
    def _get_metadata(self, image_path: str) -> Dict[str, Any]:
        """Get or cache metadata for an image."""
        if image_path in self._file_cache:
            cached_dt, _ = self._file_cache[image_path]
            return {'datetime': cached_dt}
        
        # Import metadata extractor
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
        from metadata_extractor import get_image_metadata
        
        metadata = get_image_metadata(image_path)
        desc = self._read_description(image_path) or ""
        self._file_cache[image_path] = (metadata.get('datetime'), desc)
        
        return metadata
    
    def _get_all_images(self) -> List[str]:
        """Get all image files from shared cache (populated during load())."""
        return ContextTask._shared_images_cache or []
    
    def _discover_images(self, root_dir: str) -> List[str]:
        """Discover all image files recursively."""
        from config_loader import DEFAULT_IMAGE_EXTENSIONS
        
        images: List[str] = []
        for dirpath, _, filenames in os.walk(root_dir):
            for filename in filenames:
                if any(filename.lower().endswith(ext) for ext in DEFAULT_IMAGE_EXTENSIONS):
                    images.append(os.path.join(dirpath, filename))
        
        return images
    
    def _calculate_relevance_score(
        self,
        target_path: str,
        target_datetime: Optional[datetime],
        target_dir: str,
        target_filename: str,
        candidate_path: str,
        candidate_metadata: Dict[str, Any]
    ) -> float:
        """
        Calculate relevance score for a context candidate.
        Higher score = more relevant context.
        """
        score = 0.0
        candidate_datetime = candidate_metadata.get('datetime')
        candidate_dir = os.path.dirname(candidate_path)
        candidate_filename = os.path.basename(candidate_path)
        
        # Directory similarity (strongest signal)
        if candidate_dir == target_dir:
            score += 100.0  # Same directory
        elif candidate_dir.startswith(target_dir) or target_dir.startswith(candidate_dir):
            score += 50.0  # Parent/child directory
        else:
            # Count common path components
            target_parts = target_dir.split(os.sep)
            candidate_parts = candidate_dir.split(os.sep)
            common = sum(1 for a, b in zip(target_parts, candidate_parts) if a == b)
            score += common * 5.0
        
        # Temporal proximity (if both have datetime)
        if target_datetime and candidate_datetime:
            time_diff = abs((target_datetime - candidate_datetime).total_seconds())
            days_diff = time_diff / 86400.0  # Convert to days
            
            if days_diff <= self.context_window_days:
                # Within window: score decreases with distance
                temporal_score = 50.0 * (1.0 - (days_diff / self.context_window_days))
                score += temporal_score
            else:
                # Outside window: return 0 to exclude
                return 0.0
        
        # Filename similarity (weak signal but helpful)
        # Strip extensions and numbers for base comparison
        import re
        target_base = re.sub(r'\d+', '', os.path.splitext(target_filename)[0])
        candidate_base = re.sub(r'\d+', '', os.path.splitext(candidate_filename)[0])
        
        if target_base and candidate_base:
            # Simple character overlap
            common_chars = set(target_base.lower()) & set(candidate_base.lower())
            if common_chars:
                score += len(common_chars) * 0.5
        
        return score
