import json
import os
import sys
import tempfile
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from typing import Tuple, Dict, Any
from describe_media.tasks.task import Task
from describe_media.tasks.media import output_relative_path
from PIL import Image, ImageOps


class ResizeTask(Task[Tuple[str, Dict[str, Any]], Tuple[str, Dict[str, Any]]]):
    """
    Resize images to max 720p while maintaining aspect ratio.
    
    Only resizes images larger than the max dimensions.
    Saves as .resized-720p.jpg in the output directory.
    
    Input: path to image
    Output: path to resized image or original if no resize needed
    """
    
    def __init__(
        self,
        maximum: int,
        input_dir: str,
        output_dir: str,
        max_width: int = 720,
        max_height: int = 720,
        retry: bool = False,
        retry_failed: bool = False,
    ) -> None:
        super().__init__(maximum, input_dir=input_dir)
        self.output_dir: str = output_dir
        self.max_width: int = max_width
        self.max_height: int = max_height
        self.retry: bool = retry
        self.retry_failed = retry_failed
    
    def execute(self, item: Tuple[str, Dict[str, Any]]) -> Tuple[str, Dict[str, Any]]:
        """
        Resize image if larger than max dimensions, maintaining aspect ratio.
        Uses the best available version as source.
        
        Args: item (path, metadata)
        Returns: (resized_path or source_path, metadata)
        """
        input_path, metadata = item
        metadata = dict(metadata)
        
        try:
            relative = output_relative_path(input_path, self.input_dir, metadata)
            state_path = os.path.join(self.output_dir, relative + ".resize.json")
            error_path = os.path.join(self.output_dir, relative + ".resize.error.json")
            if not self.retry and os.path.exists(state_path):
                with open(state_path, "r", encoding="utf-8") as handle:
                    state = json.load(handle)
                prepared_path = state.get("prepared_image_path")
                if isinstance(prepared_path, str) and os.path.exists(prepared_path):
                    self.record_skip()
                    self._set_prepared_image_metadata(metadata, relative, prepared_path)
                    metadata["_stage"] = "resize"
                    return input_path, metadata
            if not self.retry and not self.retry_failed and os.path.exists(error_path):
                self.record_skip()
                return input_path, {**metadata, "_stage": "resize-blocked"}

            # Use the best available image as source for resizing
            source_path = metadata.get("_fixed_image_path") or self.get_preferred_image_path(input_path)
            
            with Image.open(source_path) as img:
                # Apply EXIF orientation to correct image rotation/flips
                img = ImageOps.exif_transpose(img)
                
                # Extract EXIF data for preservation
                exif_data = img.getexif()
                icc_profile = img.info.get('icc_profile')
                
                width, height = img.size
                
                # Check if resize is needed
                if width <= self.max_width and height <= self.max_height:
                    # No resize needed, return the source path with metadata
                    self._set_prepared_image_metadata(metadata, relative, source_path)
                    self._write_state(state_path, relative, source_path)
                    metadata["_stage"] = "resize"
                    return (input_path, metadata)
                
                # Calculate new size maintaining aspect ratio
                ratio = min(self.max_width / width, self.max_height / height)
                new_width = int(width * ratio)
                new_height = int(height * ratio)
                
                # Calculate output path
                base = os.path.splitext(relative)[0]
                output_path = os.path.join(self.output_dir, base + ".resized.jpg")
                
                # Skip if already resized (unless --retry flag is set)
                if not self.retry and os.path.exists(output_path):
                    self.record_skip()
                    self._set_prepared_image_metadata(metadata, relative, output_path)
                    self._write_state(state_path, relative, output_path)
                    metadata["_stage"] = "resize"
                    return (input_path, metadata)
                
                # Resize image
                resized_img = img.resize((new_width, new_height), Image.Resampling.LANCZOS)
                
                # Create directories if needed
                os.makedirs(os.path.dirname(output_path), exist_ok=True)
                
                # Prepare save parameters
                save_kwargs = {'quality': 95, 'optimize': True}
                if exif_data:
                    save_kwargs['exif'] = exif_data
                if icc_profile:
                    save_kwargs['icc_profile'] = icc_profile
                
                # Convert RGBA to RGB if needed for JPEG
                if resized_img.mode in ('RGBA', 'LA', 'P'):
                    rgb_img = Image.new('RGB', resized_img.size, (255, 255, 255))
                    rgb_img.paste(resized_img, mask=resized_img.split()[-1] if resized_img.mode == 'RGBA' else None)
                    resized_img = rgb_img
                
                resized_img.save(output_path, 'JPEG', **save_kwargs)

                self._set_prepared_image_metadata(metadata, relative, output_path)
                self._write_state(state_path, relative, output_path)
                try:
                    os.remove(error_path)
                except FileNotFoundError:
                    pass
                metadata["_stage"] = "resize"
                return (input_path, metadata)
            
        except Exception as e:
            # Show relative path in error
            rel_path = input_path
            if self.input_dir and input_path.startswith(self.input_dir):
                try:
                    rel_path = os.path.relpath(input_path, self.input_dir)
                except (ValueError, TypeError):
                    pass
            try:
                self._write_error(error_path, relative, e)
            except Exception:
                pass
            raise Exception(f"Failed to resize {rel_path}: {str(e)}")

    def _set_prepared_image_metadata(self, metadata: Dict[str, Any], relative: str, prepared_path: str) -> None:
        """Expose a resized artifact as an additional shortcut destination."""
        metadata["_prepared_image_path"] = prepared_path
        resized_path = os.path.join(self.output_dir, os.path.splitext(relative)[0] + ".resized.jpg")
        if os.path.normcase(os.path.abspath(prepared_path)) == os.path.normcase(os.path.abspath(resized_path)):
            metadata["_shortcut_output_relative_path"] = os.path.relpath(prepared_path, self.output_dir).replace("\\", "/")
        else:
            metadata.pop("_shortcut_output_relative_path", None)

    @staticmethod
    def _write_state(path: str, relative: str, prepared_path: str) -> None:
        ResizeTask._write_json(path, {"schema_version": 1, "source": {"relative_path": relative.replace("\\", "/")}, "prepared_image_path": prepared_path})

    @staticmethod
    def _write_error(path: str, relative: str, error: Exception) -> None:
        ResizeTask._write_json(path, {"schema_version": 1, "status": "error", "source": {"relative_path": relative.replace("\\", "/")}, "error": str(error)})

    @staticmethod
    def _write_json(path: str, payload: Dict[str, Any]) -> None:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        descriptor, temporary_path = tempfile.mkstemp(prefix=".resize-", suffix=".json", dir=os.path.dirname(path))
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
                handle.write("\n")
            os.replace(temporary_path, path)
        except Exception:
            try:
                os.unlink(temporary_path)
            except OSError:
                pass
            raise
