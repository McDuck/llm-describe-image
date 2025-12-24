import os
import sys
from typing import Tuple
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from tasks.task import Task


class ShortcutTask(Task[str, str]):
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
    
    def execute(self, input_path: str) -> str:
        """
        Create a Windows shortcut (.lnk file) pointing to the original image.
        
        Args: input_path (path to original image)
        Returns: input_path
        """
        try:
            import win32com.client
            
            # Calculate shortcut path in output directory
            relative = os.path.relpath(input_path, self.input_dir)
            shortcut_path = os.path.join(self.output_dir, relative + ".lnk")
            
            # Create directories if needed
            os.makedirs(os.path.dirname(shortcut_path), exist_ok=True)
            
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
            return self._create_shortcut_alternative(input_path)
        except Exception as e:
            # Show relative path in error
            rel_path = input_path
            if self.input_dir and input_path.startswith(self.input_dir):
                try:
                    rel_path = os.path.relpath(input_path, self.input_dir)
                except (ValueError, TypeError):
                    pass
            raise Exception(f"Failed to create shortcut for {rel_path}: {str(e)}")
    
    def _create_shortcut_alternative(self, input_path: str) -> str:
        """
        Alternative method if win32com is not available.
        Uses shell command to create shortcut.
        """
        try:
            relative = os.path.relpath(input_path, self.input_dir)
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
