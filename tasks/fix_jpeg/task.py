import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from tasks.task import Task
from PIL import Image


class FixJpegTask(Task[str, tuple]):
    """
    Fix corrupted JPEG files by re-encoding them.
    
    Only processes files that have corruption issues.
    Returns a tuple (needs_fixing, output_path) where:
    - needs_fixing=False means file was already valid (rejected)
    - needs_fixing=True means file was corrupted and fixed
    
    Input: path to JPEG (may or may not be corrupted)
    Output: (needs_fixing: bool, path: str) - path to fixed JPEG or original
    """
    
    def __init__(
        self,
        maximum: int,
        input_dir: str,
        output_dir: str
    ) -> None:
        super().__init__(maximum, input_dir=input_dir)
        self.output_dir: str = output_dir
    
    def execute(self, input_path: str) -> str:
        """
        Fix a corrupted JPEG by re-encoding it.
        
        Only fixes files that have a JPEG-related error in their .error.txt file.
        Preserves EXIF metadata, ICC profile, and file creation/modification timestamps.
        Saves with the same quality as the original.
        
        Args: input_path (path to JPEG)
        Returns: (needs_fixing, output_path) - tuple where:
                 needs_fixing=False means rejected (no JPEG error in .error.txt)
                 needs_fixing=True means fixed (had JPEG error)
        """
        try:
            
            # Check if there's an .error.txt file with JPEG-related error
            relative = os.path.relpath(input_path, self.input_dir)
            error_file_path = os.path.join(self.output_dir, relative + ".error.txt")

            # Check if error file exists and contains JPEG-related error
            has_jpeg_error = False
            if os.path.exists(error_file_path):
                try:
                    with open(error_file_path, 'r', encoding='utf-8') as f:
                        error_content = f.read()
                        # Check for common JPEG corruption errors
                        jpeg_errors = [
                            'Chat response error: VipsJpeg: Invalid SOS parameters for sequential JPEG'
                        ]
                        has_jpeg_error = any(err in error_content for err in jpeg_errors)
                except Exception:
                    pass
            
            # If no JPEG error found, reject this file (don't process)
            if not has_jpeg_error:
                raise Exception(".error.txt does not contain 'Chat response error: VipsJpeg: Invalid SOS parameters for sequential JPEG'")
            
            # File has JPEG error, proceed with fixing
            # Get file timestamps before processing
            stat_info = os.stat(input_path)
            mod_time = stat_info.st_mtime
            
            # Calculate output path with .fixed.<extension> suffix
            relative = os.path.relpath(input_path, self.input_dir)
            base, ext = os.path.splitext(relative)
            # Create .fixed.<extension> filename
            fixed_relative = base + ".fixed" + ext
            output_path = os.path.join(self.output_dir, fixed_relative)
            
            # Create directories if needed
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Try to open and re-encode with PIL
            # PIL is more resilient to JPEG corruption
            exif_data = None
            icc_profile = None
            quality = 95  # Default quality
            
            with Image.open(input_path) as img:
                # Extract quality from original image
                if 'quality' in img.info:
                    quality = img.info['quality']
                
                # Extract EXIF data if present
                try:
                    exif_data = img.getexif()
                except Exception:
                    exif_data = None
                
                # Extract ICC profile (color profile) if present
                try:
                    icc_profile = img.info.get('icc_profile')
                except Exception:
                    icc_profile = None
                
                # Convert RGBA to RGB if needed
                if img.mode in ('RGBA', 'LA', 'P'):
                    rgb_img = Image.new('RGB', img.size, (255, 255, 255))
                    rgb_img.paste(img, mask=img.split()[-1] if img.mode == 'RGBA' else None)
                    img = rgb_img
                
                # Save with original quality and metadata
                save_kwargs = {'quality': quality, 'optimize': False}
                if exif_data:
                    save_kwargs['exif'] = exif_data
                if icc_profile:
                    save_kwargs['icc_profile'] = icc_profile
                
                # Preserve other metadata from info dict
                # Common JPEG metadata fields to preserve
                metadata_fields = ['comment', 'description', 'copyright', 'artist']
                for field in metadata_fields:
                    if field in img.info:
                        try:
                            save_kwargs[field] = img.info[field]
                        except Exception:
                            pass
                
                img.save(output_path, 'JPEG', **save_kwargs)

                # Remove error file upon image creation
                if os.path.exists(error_file_path):
                    try:
                        os.remove(error_file_path)
                    except Exception:
                        pass  # Ignore errors removing error file
            
            # Preserve original file timestamps
            try:
                os.utime(output_path, (mod_time, mod_time))
            except Exception:
                # If we can't set timestamps, continue anyway
                pass
            
            if (relative == "2025\\2025-06\\2025-06-01\\20250601_185104.jpg"):
                print(f"done")
            # Return as processed (needs_fixing=True)
            return output_path
            
        except Exception as e:
            # Show relative path in error
            rel_path = input_path
            if self.input_dir and input_path.startswith(self.input_dir):
                try:
                    rel_path = os.path.relpath(input_path, self.input_dir)
                except (ValueError, TypeError):
                    pass
            raise Exception(f"Failed to fix JPEG {rel_path}: {str(e)}")
