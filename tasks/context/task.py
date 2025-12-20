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
        """Initialize without discovering all images (lazy loading)."""
        super().load()
        # Don't pre-cache all 160K images - discover on-demand per worker
    
    def execute(self, input_path: str) -> Tuple[str, str, List[str]]:
        """
        Gather context descriptions from nearby images.
        Args: input_path
        Returns: (input_path, original_description, context_descriptions)
        
        Note: Reads original description (not context-enhanced version).
        The original description should exist from a previous describe pipeline run.
        """
        # Read the original description (plain .txt, not context-enhanced)
        original_desc = self._read_description(input_path, use_original=True)
        if not original_desc:
            # Skip images without original descriptions
            return (input_path, "", [])
        
        # Get metadata for target image
        from tasks.download.metadata_extractor import get_image_metadata
        target_metadata = get_image_metadata(input_path)
        target_dir = os.path.dirname(input_path)
        target_filename = os.path.basename(input_path)
        
        # Get candidate images from nearby folders only (not all 2300+ images)
        candidates_to_check = self._get_nearby_images(
            input_path,
            self.input_dir,
            target_metadata
        )
        
        # If no candidates found, return with empty context
        if not candidates_to_check:
            return (input_path, original_desc, [], original_desc, [])
        
        # Score and filter context candidates (only from pre-filtered set)
        context_candidates: List[Tuple[Tuple[float, float], str, str, str]] = []  # ((min_score, max_score), path, full_desc, desc_content)
        
        found_count = 0
        desc_count = 0
        
        for score, img_path in candidates_to_check:
            found_count += 1
            
            desc = self._read_description(img_path, use_original=True)
            if not desc:
                # Reject task if any candidate lacks description
                return (input_path, original_desc, [], original_desc, [])
            
            desc_count += 1
            
            if score[1] < float('inf'):  # Check max value isn't infinity
                context_candidates.append((score, img_path, desc, desc))
        
        # Candidates are already sorted by relevance from _get_nearby_images
        # Return both full descriptions (for debug) and content-only (for prompt)
        context_full_descs = [full_desc for _, _, full_desc, _ in context_candidates[:self.max_context_items]]
        context_descriptions = [desc for _, _, _, desc in context_candidates[:self.max_context_items]]
        
        return (input_path, original_desc, context_descriptions, original_desc, context_full_descs)
    
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
                content = f.read().strip()
                return content
        except Exception:
            return None
    
    def _extract_description_content(self, formatted_text: str) -> Optional[str]:
        """
        Extract just the description content from formatted output.
        Looks for content after "Beschrijving:" or "Verbeterde beschrijving:" headers.
        """
        # Try to find "Beschrijving:" header
        markers = ["Verbeterde beschrijving:", "Beschrijving:"]
        
        for marker in markers:
            if marker in formatted_text:
                # Split by marker and get the part after it
                parts = formatted_text.split(marker, 1)
                if len(parts) > 1:
                    content = parts[1].strip()
                    return content if content else None
        
        # If no marker found, assume entire text is description
        return formatted_text if formatted_text else None
    

    def _get_nearby_images(self, input_path: str, root_path: str, target_metadata: Dict[str, Any]) -> List[Tuple[Tuple[float, float], str]]:
        """
        Get images closest to the input file in natural sort order.
        
        Collects images in two passes:
        - First: images >= target path (sorted naturally)
        - Second: images <= target path (sorted naturally)
        
        Combines both, sorts everything, finds the target's position,
        and returns N images before and N images after it.
        N is determined by max_context_items // 2.
        
        Returns: list of image paths in natural sort order around the target file
        """
        try:
            from config_loader import DEFAULT_IMAGE_EXTENSIONS
            
            normalized_input_path: str = os.path.normpath(input_path)
            context_count: int = self.max_context_items // 2
            current_dir: str = os.path.dirname(normalized_input_path)
            all_images: List[str] = []
            
            # Collect images >= target path by expanding from target directory upward
            dirs_to_discover: Dict[str, bool] = {current_dir: True}
            all_up_images: List[str] = []

            while any(dirs_to_discover.values()) and len(all_up_images) < self.max_context_items * 2:
                # Find the directory that still needs to be discovered with the highest natural sort order
                current_dir_key = max((d for d, needs_discovery in dirs_to_discover.items() if needs_discovery), default=None)
                if current_dir_key is None:
                    break
                dirs_to_discover[current_dir_key] = False

                for filename in os.listdir(current_dir_key):
                    filepath = os.path.join(current_dir_key, filename)
                    if (filepath >= input_path):
                        continue
                    if (os.path.isdir(filepath)):
                        if filepath not in dirs_to_discover:
                            dirs_to_discover[filepath] = True
                        continue
                    if any(filename.lower().endswith(ext) for ext in DEFAULT_IMAGE_EXTENSIONS):
                        all_up_images.append(filepath)
                parent_dir = os.path.dirname(current_dir_key)
                if parent_dir.startswith(root_path) and parent_dir not in dirs_to_discover:
                    dirs_to_discover[parent_dir] = True
            
            # Collect images <= target path by expanding from target directory upward
            dirs_to_discover: Dict[str, bool] = {current_dir: True}
            all_down_images: List[str] = []

            while any(dirs_to_discover.values()) and len(all_down_images) < self.max_context_items * 2:
                # Find the directory that still needs to be discovered with the highest natural sort order
                current_dir_key = min((d for d, needs_discovery in dirs_to_discover.items() if needs_discovery), default=None)
                if current_dir_key is None:
                    break
                dirs_to_discover[current_dir_key] = False

                for filename in os.listdir(current_dir_key):
                    filepath = os.path.join(current_dir_key, filename)
                    if filepath <= input_path:
                        continue
                    if (os.path.isdir(filepath)):
                        if filepath not in dirs_to_discover:
                            dirs_to_discover[filepath] = True
                        continue
                    if any(filename.lower().endswith(ext) for ext in DEFAULT_IMAGE_EXTENSIONS):
                        all_down_images.append(filepath)
                
                parent_dir = os.path.dirname(current_dir_key)
                if parent_dir.startswith(root_path) and parent_dir not in dirs_to_discover:
                    dirs_to_discover[parent_dir] = True
            
            all_images = all_up_images + all_down_images

            if not all_images:
                return []

            # Score all candidates by temporal proximity
            from tasks.download.metadata_extractor import get_image_metadata
            scored_candidates: List[Tuple[Tuple[float, float], str]] = []
            
            for img_path in all_images:
                if os.path.normpath(img_path) == os.path.normpath(input_path):
                    continue
                
                candidate_metadata = get_image_metadata(img_path)
                score = self._calculate_relevance_score(target_metadata, candidate_metadata)
                scored_candidates.append((score, img_path))
            
            # Sort by score (highest/most relevant first)
            scored_candidates.sort(key=lambda x: x[0][1])
            
            return scored_candidates
        
        except Exception:
            # If something fails, just return empty list
            return []
    
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
