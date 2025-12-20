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
    _shared_metadata_cache: Dict[str, Tuple[Optional[datetime], str]] = {}  # Shared across threads
    _metadata_cache_lock: threading.Lock = threading.Lock()
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
    
    def load(self) -> None:
        """Pre-load image list before threads start (once for all threads)."""
        super().load()
        
        # Only the first thread builds the image list (not metadata - that's done on-demand)
        if not ContextTask._cache_initialized:
            with ContextTask._shared_cache_lock:
                if not ContextTask._cache_initialized:  # Double-check
                    print("Discovering images for context gathering...")
                    ContextTask._shared_images_cache = self._discover_images(self.input_dir)
                    print(f"Found {len(ContextTask._shared_images_cache)} images")
                    ContextTask._cache_initialized = True
    
    def execute(self, item: str) -> Tuple[str, str, List[str]]:
        """
        Gather context descriptions from nearby images.
        Args: input_path
        Returns: (input_path, original_description, context_descriptions)
        
        Note: Reads original description (not context-enhanced version).
        The original description should exist from a previous describe pipeline run.
        """
        input_path = item
        
        # Read the original description (plain .txt, not context-enhanced)
        original_desc = self._read_description(input_path, use_original=True)
        if not original_desc:
            # Skip images without original descriptions
            return (input_path, "", [])
        
        # Get metadata for target image
        target_metadata = self._get_metadata(input_path)
        target_datetime = target_metadata.get('datetime')
        target_dir = os.path.dirname(input_path)
        target_filename = os.path.basename(input_path)
        
        # Get all images and filter to directory+time window FIRST (pre-filter)
        all_images = self._get_all_images()
        candidates_to_check = self._pre_filter_candidates(
            input_path, target_dir, target_datetime, all_images
        )
        
        # Score and filter context candidates (only from pre-filtered set)
        context_candidates: List[Tuple[float, str, str]] = []  # (score, path, description)
        
        for img_path in candidates_to_check:
            desc = self._read_description(img_path, use_original=True)
            if not desc:
                continue  # Skip images without descriptions
            
            # Calculate relevance score
            score = self._calculate_relevance_score(
                get_image_metadata(input_path),
                target_metadata
            )
            
            if score > 0:
                context_candidates.append((score, img_path, desc))
        
        # Sort by score (highest first) and take top N
        context_candidates.sort(reverse=True, key=lambda x: x[0])
        context_descriptions = [desc for _, _, desc in context_candidates[:self.max_context_items]]
        
        return (input_path, original_desc, context_descriptions)
    
    def _read_description(self, image_path: str, use_original: bool = False) -> Optional[str]:
        """
        Read description file for an image.
        
        Args:
            image_path: Path to the image file
            use_original: If True, read the original .txt file (not context-enhanced)
                         If False, use output_suffix configured in kwargs
        """
        if self.input_dir and self.output_dir:
            relative = os.path.relpath(image_path, self.input_dir)
            
            if use_original:
                # Read original description file (plain .txt)
                desc_file = os.path.join(self.output_dir, relative + ".txt")
            else:
                # Read context-enhanced description (uses configured suffix)
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
        """Get or cache metadata for an image (uses shared cache across all threads)."""
        # Check shared cache first
        with ContextTask._metadata_cache_lock:
            if image_path in ContextTask._shared_metadata_cache:
                cached_dt, _ = ContextTask._shared_metadata_cache[image_path]
                return {'datetime': cached_dt}
        
        # Import metadata extractor (located in download task)
        from tasks.download.metadata_extractor import get_image_metadata
        
        metadata = get_image_metadata(image_path)
        desc = self._read_description(image_path) or ""
        
        # Store in shared cache
        with ContextTask._metadata_cache_lock:
            ContextTask._shared_metadata_cache[image_path] = (metadata.get('datetime'), desc)
        
        return metadata
    
    def _get_all_images(self) -> List[str]:
        """Get all image files from shared cache (populated during load())."""
        return ContextTask._shared_images_cache or []
    
    def _pre_filter_candidates(
        self,
        target_path: str,
        target_dir: str,
        target_datetime: Optional[datetime],
        all_images: List[str]
    ) -> List[str]:
        """
        Pre-filter candidates to reduce search space.
        First filter by directory proximity, then by time window.
        Returns much smaller candidate list for full scoring.
        """
        candidates = []
        
        for img_path in all_images:
            if img_path == target_path:
                continue  # Skip self
            
            img_dir = os.path.dirname(img_path)
            
            # DIRECTORY FILTER: Only include if in same or nearby directories
            # Same directory or adjacent date folders (YYYY/YYYY-MM/YYYY-MM-DD)
            target_parts = target_dir.split(os.sep)
            img_parts = img_dir.split(os.sep)
            
            # Must share at least YYYY/YYYY-MM (year and month)
            min_common_depth = 2  # Year and month folders
            common_depth = sum(1 for a, b in zip(target_parts, img_parts) if a == b)
            
            if common_depth < min_common_depth:
                continue  # Different months - skip
            
            # TIME WINDOW FILTER: Quick metadata check (only if needed)
            if target_datetime and self.context_window_days > 0:
                img_metadata = self._get_metadata(img_path)
                img_datetime = img_metadata.get('datetime')
                
                if img_datetime:
                    time_diff = abs((target_datetime - img_datetime).total_seconds())
                    days_diff = time_diff / 86400.0
                    
                    if days_diff > self.context_window_days:
                        continue  # Outside time window
            
            candidates.append(img_path)
        
        return candidates
    
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
        target_metadata: Dict[str, Any],
        candidate_metadata: Dict[str, Any]
    ) -> Tuple[float, float]:
        """
        Calculate relevance score range (min, max) for a context candidate.
        Based only on temporal proximity (time difference between images).
        Returns: (min_score, max_score) tuple representing score range.
        Accounts for datetime uncertainty from both target and candidate images.
        """
        target_datetime = target_metadata.get('datetime')
        target_datetime_min = target_metadata.get('datetime_min')
        target_datetime_max = target_metadata.get('datetime_max')
        
        candidate_datetime = candidate_metadata.get('datetime')
        candidate_datetime_min = candidate_metadata.get('datetime_min')
        candidate_datetime_max = candidate_metadata.get('datetime_max')
        
        # Only temporal proximity scoring
        if target_datetime and candidate_datetime:
            # Use datetime ranges for both target and candidate for accurate uncertainty bounds
            # Min score: shortest possible distance (target_max to candidate_min)
            # Max score: longest possible distance (target_min to candidate_max)
            time_diff_min = abs((target_datetime_max - candidate_datetime_min).total_seconds())
            time_diff_max = abs((target_datetime_min - candidate_datetime_max).total_seconds())
            return (time_diff_min, time_diff_max)
        else:
            # No datetime available: exclude
            return (float('inf'), float('inf'))
