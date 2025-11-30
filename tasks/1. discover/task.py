import os
import re
import sys
import heapq
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from tasks.task import Task


class DiscoverTask(Task):
    def __init__(self, maximum=1, input_dir=None, image_extensions=None, sort_order="natural-desc"):
        # Initialize internal attributes first
        object.__setattr__(self, '_heap_queue', [])
        object.__setattr__(self, 'counter', 0)
        
        super().__init__(maximum, input_dir=input_dir)
        self.image_extensions = image_extensions or {".jpg", ".jpeg", ".png", ".webp"}
        self.sort_order = sort_order
        self.pending_queue = []  # Items to add to queue after finish()
    
    def add(self, item):
        """Add item to priority queue with depth-based priority (deeper = higher priority)."""
        depth = item.count(os.sep)  # Deeper paths have more separators
        # Negate depth so deeper paths (higher depth) have lower values = higher priority
        heapq.heappush(self._heap_queue, (-depth, self.counter, item))
        self.counter += 1
    
    def start_next(self, next_task=None, backpressure_multiplier=2.0):
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
            threshold = next_task.maximum * backpressure_multiplier
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
    def queue(self):
        """Return list of items in queue for compatibility."""
        return [item for _, _, item in self._heap_queue]
    
    @queue.setter
    def queue(self, value):
        """Ignore queue assignments from parent class."""
        pass

    def execute(self, directory_path):
        """
        Discover image files in a directory.
        Returns list of discovered file paths.
        Also queues subdirectories for future discovery.
        """
        discovered_files = []
        
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
            entries = os.listdir(directory_path)
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
        files = []
        dirs = []
        for entry in entries:
            entry_path = os.path.join(directory_path, entry)
            if os.path.isfile(entry_path):
                files.append(entry)
            elif os.path.isdir(entry_path):
                dirs.append(entry_path)
        
        # Sort files
        def _natural_key(s: str):
            return [int(t) if t.isdigit() else t.lower() for t in re.split(r'(\d+)', s)]
        
        if self.sort_order.startswith("natural"):
            key_fn = _natural_key
        else:
            key_fn = lambda s: s.lower()
        
        rev = self.sort_order.endswith("desc")
        files = sorted(files, key=key_fn, reverse=rev)
        dirs = sorted(dirs, key=key_fn, reverse=rev)
        
        # Filter image files
        discovered_files = []
        for file in files:
            file_path = os.path.join(directory_path, file)
            ext = os.path.splitext(file)[1].lower()
            if ext in self.image_extensions:
                discovered_files.append(file_path)
        
        # Return tuple: (files, subdirs to queue)
        return (discovered_files, dirs)
