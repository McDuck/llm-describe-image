import os
import sys
from typing import Any, Tuple
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from describe_media.tasks.task import Task
from describe_media.tasks.media import output_relative_path, split_media_item


class ShortcutTask(Task[Any, str]):
    """
    Create Windows shortcuts (.lnk files) linking to original images.
    
    Input: image path
    Output: image path (after shortcut created)
    """
    
    def __init__(
        self,
        maximum: int,
        input_dir: str,
        output_dir: str
    ) -> None:
        super().__init__(maximum, input_dir=input_dir)
        self.output_dir: str = output_dir
    
    def execute(self, item: Any) -> str:
        """
        Create a Windows shortcut (.lnk file) pointing to the original image.
        
        Args: input_path (path to original image)
        Returns: input_path
        """
        input_path, metadata = split_media_item(item)
        try:
            # Calculate shortcut path in output directory
            relative = self._shortcut_output_relative_path(input_path, metadata)
            shortcut_path = os.path.join(self.output_dir, relative + ".lnk")
            if os.path.exists(shortcut_path):
                self.record_skip()
                return input_path
            
            # Create directories if needed
            os.makedirs(os.path.dirname(shortcut_path), exist_ok=True)

            if os.name != "nt":
                self._create_portable_windows_shortcut(shortcut_path, input_path)
                return input_path

            import win32com.client

            # Get absolute path for the target
            target_path = os.path.abspath(input_path)
            
            # Create the shortcut
            shell = win32com.client.Dispatch("WScript.Shell")
            shortcut = shell.CreateShortCut(shortcut_path)
            shortcut.TargetPath = target_path
            shortcut.WorkingDirectory = os.path.dirname(target_path)
            shortcut.save()
            
            return input_path
            
        except ImportError:
            # win32com not available - try alternative method
            return self._create_shortcut_alternative(input_path, metadata)
        except Exception as e:
            # Show relative path in error
            rel_path = input_path
            if self.input_dir and input_path.startswith(self.input_dir):
                try:
                    rel_path = os.path.relpath(input_path, self.input_dir)
                except (ValueError, TypeError):
                    pass
            raise Exception(f"Failed to create shortcut for {rel_path}: {str(e)}")
    
    def _create_shortcut_alternative(self, input_path: str, metadata: Any = None) -> str:
        """
        Alternative method if win32com is not available.
        Uses shell command to create shortcut.
        """
        try:
            relative = self._shortcut_output_relative_path(input_path, metadata or {})
            shortcut_path = os.path.join(self.output_dir, relative + ".lnk")
            os.makedirs(os.path.dirname(shortcut_path), exist_ok=True)
            
            target_path = os.path.abspath(input_path)
            
            # Use PowerShell to create shortcut
            ps_command = f'''
$shell = New-Object -ComObject WScript.Shell
$shortcut = $shell.CreateShortcut('{shortcut_path}')
$shortcut.TargetPath = '{target_path}'
$shortcut.WorkingDirectory = '{os.path.dirname(target_path)}'
$shortcut.Save()
'''
            
            import subprocess
            subprocess.run(
                ["powershell", "-Command", ps_command],
                check=True,
                capture_output=True
            )
            
            return input_path
            
        except Exception as e:
            rel_path = input_path
            if self.input_dir and input_path.startswith(self.input_dir):
                try:
                    rel_path = os.path.relpath(input_path, self.input_dir)
                except (ValueError, TypeError):
                    pass
            raise Exception(f"Failed to create shortcut via PowerShell for {rel_path}: {str(e)}")

    @staticmethod
    def _create_portable_windows_shortcut(shortcut_path: str, target_path: str) -> None:
        """Write a Windows Shell Link that resolves its target relatively.

        Linux containers cannot use Windows COM or PowerShell.  A relative link
        keeps the shortcut valid when both the output directory and source
        album are opened through the storage share in Windows.
        """
        relative_target = os.path.relpath(target_path, os.path.dirname(shortcut_path))
        flags = 0x00000008 | 0x00000080  # HasRelativePath | IsUnicode
        header_tail = b"\x00" * 36 + (1).to_bytes(4, "little") + b"\x00" * 12
        header = (
            (0x4C).to_bytes(4, "little")
            + bytes.fromhex("0114020000000000c000000000000046")
            + flags.to_bytes(4, "little")
            + header_tail
        )
        encoded_target = relative_target.replace("/", "\\").encode("utf-16-le")
        with open(shortcut_path, "wb") as shortcut_file:
            shortcut_file.write(header + (len(encoded_target) // 2).to_bytes(2, "little") + encoded_target)

    def _shortcut_output_relative_path(self, input_path: str, metadata: Any) -> str:
        """Use an explicit output identity for generated media such as resized images."""
        if isinstance(metadata, dict):
            relative = metadata.get("_shortcut_output_relative_path")
            if isinstance(relative, str) and relative:
                return relative
        return output_relative_path(input_path, self.input_dir, metadata if isinstance(metadata, dict) else {})
