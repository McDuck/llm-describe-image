import os
import re
import sys
import heapq
from typing import Optional, Set, List, Tuple, Any
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from tasks.task import Task


class DiscoverTask(Task[str, Tuple[List[str], List[str]]]):
    def __init__(self, maximum: int = 1, input_dir: Optional[str] = None, image_extensions: Optional[Set[str]] = None, sort_order: str = "natural-desc") -> None:
        # Initialize internal attributes first
        object.__setattr__(self, '_heap_queue', [])
        object.__setattr__(self, 'counter', 0)
        
        super().__init__(maximum, input_dir=input_dir)
        self.image_extensions: Set[str] = image_extensions or {".jpg", ".jpeg", ".png", ".webp"}
        self.sort_order: str = sort_order
        self.pending_queue: List[str] = []  # Items to add to queue after finish()
        self._heap_queue: List[Tuple[int, int, str]]
        self.counter: int
    
    def add(self, item: str) -> None:
        """Add item to priority queue with depth-based priority (deeper = higher priority)."""
        depth: int = item.count(os.sep)  # Deeper paths have more separators
        # Negate depth so deeper paths (higher depth) have lower values = higher priority
        heapq.heappush(self._heap_queue, (-depth, self.counter, item))
        self.counter += 1
    
    def start_next(self, next_task: Optional[Task] = None, backpressure_multiplier: float = 2.0) -> Optional[str]:
        """
        Move next item from priority queue to active and return it if capacity allows.
        
        Args:
            next_task: Optional next task in pipeline. If provided, checks if downstream has capacity.
            backpressure_multiplier: Allow downstream queue to grow to (max * multiplier) before stopping.
        """
        # Check if we have capacity
        if len(self.active) >= self.maximum or not self._heap_queue:
            return None
        
        # Check if next task has capacity (only check queue, allow it to build up)
        if next_task is not None:
            threshold: float = next_task.maximum * backpressure_multiplier
            if len(next_task.queue) >= threshold:
                # Downstream queue is too full, don't start more work
                return None
        
        # Pop from heap and activate
        if self._heap_queue:
            _, _, item = heapq.heappop(self._heap_queue)
            self.active.append(item)
            return item
        return None
    
    @property
    def queue(self) -> List[str]:
        """Return list of items in queue for compatibility."""
        return [item for _, _, item in self._heap_queue]
    
    @queue.setter
    def queue(self, value: List[str]) -> None:
        """Ignore queue assignments from parent class."""
        pass

    def execute(self, directory_path: str) -> Tuple[List[str], List[str]]:
        """
        Discover image files in a directory.
        Returns list of discovered file paths.
        Also queues subdirectories for future discovery.
        """
        discovered_files: List[str] = []
        
        if not os.path.isdir(directory_path):
            # Show relative path in error
            rel_path = directory_path
            if self.input_dir and directory_path.startswith(self.input_dir):
                try:
                    rel_path = os.path.relpath(directory_path, self.input_dir)
                except (ValueError, TypeError):
                    pass
            try:
                print(f"Error: Not a directory - {rel_path}")
            except:
                pass  # Ignore print errors during shutdown
            return discovered_files
        
        # List directory contents
        try:
            entries: List[str] = os.listdir(directory_path)
        except Exception as e:
            # Show relative path in error
            rel_path = directory_path
            if self.input_dir and directory_path.startswith(self.input_dir):
                try:
                    rel_path = os.path.relpath(directory_path, self.input_dir)
                except (ValueError, TypeError):
                    pass
            try:
                print(f"Error listing directory {rel_path}: {e}")
            except:
                pass  # Ignore print errors during shutdown
            return discovered_files
        
        # Separate files and directories
        files: List[str] = []
        dirs: List[str] = []
        for entry in entries:
            entry_path: str = os.path.join(directory_path, entry)
            if os.path.isfile(entry_path):
                files.append(entry)
            elif os.path.isdir(entry_path):
                dirs.append(entry_path)
        
        # Sort files
        def _natural_key(s: str) -> List[Any]:
            return [int(t) if t.isdigit() else t.lower() for t in re.split(r'(\d+)', s)]
        
        if self.sort_order.startswith("natural"):
            key_fn = _natural_key
        else:
            key_fn = lambda s: s.lower()
        
        rev: bool = self.sort_order.endswith("desc")
        files = sorted(files, key=key_fn, reverse=rev)
        dirs = sorted(dirs, key=key_fn, reverse=rev)
        
        # Filter image files
        discovered_files: List[str] = []
        for file in files:
            file_path: str = os.path.join(directory_path, file)
            ext: str = os.path.splitext(file)[1].lower()
            if ext in self.image_extensions:
                discovered_files.append(file_path)
        
        # Return tuple: (files, subdirs to queue)
        return (discovered_files, dirs)
