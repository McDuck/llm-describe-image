import os
from typing import Optional, List, Any, TypeVar, Generic

InputType = TypeVar('InputType')
OutputType = TypeVar('OutputType')


class TaskStats:
    """Statistics for a task including input/output counts, failures, and rejections."""
    def __init__(self) -> None:
        self.diff_input_output: bool = False
        self.done: int = 0
        self.failed: int = 0
        self.input: int = 0
        self.output: int = 0
        self.rejected: int = 0
    
    def reset(self) -> None:
        """Reset all counters to zero."""
        self.done = 0
        self.failed = 0
        self.input = 0
        self.output = 0
        self.rejected = 0
    
    def finish(self, output_count: int = 1) -> None:
        """Record a finished item with output count."""
        self.done += 1
        self.input += 1
        if (output_count != 1):
            self.diff_input_output = True
        self.output += output_count
    
    def fail(self) -> None:
        """Record a failed item."""
        self.failed += 1
        self.input += 1
    
    def reject(self) -> None:
        """Record a rejected item."""
        self.rejected += 1
        self.input += 1
    
    def format(self) -> str:
        """Format stats as string: [input>][failed F][rejected R]output D"""
        fmt = ""
        if self.diff_input_output:
            fmt = f"{self.input}>"
        if self.failed > 0:
            fmt = f"{fmt}{self.failed}F"
        if self.rejected > 0:
            fmt = f"{fmt}{self.rejected}R"
        fmt = f"{fmt}{self.output}D"
        return fmt


class Task(Generic[InputType, OutputType]):
    def __init__(self, maximum: int = 1, input_dir: Optional[str] = None) -> None:
        self.queue: List[InputType] = []          # items waiting
        self.active: List[InputType] = []         # items processing
        self.maximum: int = maximum   # max active allowed
        self.input_dir: Optional[str] = input_dir  # for relative path display

        # Stats objects
        self.recent: TaskStats = TaskStats()
        self.total: TaskStats = TaskStats()

    # --- Lifecycle Methods ---------------------------------------------------
    
    def load(self) -> None:
        """Load resources at worker thread start. Override in subclasses if needed."""
        pass
    
    def unload(self) -> None:
        """Unload resources at worker thread end. Override in subclasses if needed."""
        pass

    # --- Utility Methods-----------------------------------------------------
    
    @staticmethod
    def get_preferred_image_path(input_path: str) -> str:
        """
        Check if a .fixed version of the image exists and return it.
        Otherwise return the original input path.
        
        Used to prefer fixed images (from fix_jpeg task) over corrupted originals.
        
        Args:
            input_path: Original image path
        
        Returns:
            Path to .fixed image if it exists, otherwise original input_path
        """
        base, ext = os.path.splitext(input_path)
        fixed_path = base + ".fixed" + ext
        
        if os.path.exists(fixed_path):
            return fixed_path
        return input_path

    # --- Queue Management ----------------------------------------------------

    def add(self, item: InputType) -> None:
        """Add an item to the queue."""
        self.queue.append(item)

    def start_next(self, next_task: Optional['Task[InputType, OutputType]'] = None, backpressure_multiplier: float = 2.0) -> Optional[InputType]:
        """
        Move next item from queue to active
        and return it if capacity allows.
        
        Args:
            next_task: Optional next task in pipeline. If provided, checks if downstream has capacity.
            backpressure_multiplier: Allow downstream queue to grow to (max * multiplier) before stopping.
        """
        # Check if we have capacity
        if len(self.active) >= self.maximum or not self.queue:
            return None
        
        # Check if next task has capacity (only check queue, allow it to build up)
        if next_task is not None:
            threshold = next_task.maximum * backpressure_multiplier
            if len(next_task.queue) >= threshold:
                # Downstream queue is too full, don't start more work
                return None
        
        # Start the item
        item = self.queue.pop(0)
        self.active.append(item)
        return item

    def finish(self, item: InputType, output_count: int = 1) -> None:
        """Mark as done with output count."""
        if item in self.active:
            self.active.remove(item)
        self.recent.finish(output_count)
        self.total.finish(output_count)

    def fail(self, item: InputType) -> None:
        """Mark as failed."""
        if item in self.active:
            self.active.remove(item)
        self.recent.fail()
        self.total.fail()

    def reject(self, item: InputType) -> None:
        """Mark as rejected (e.g., skipped)."""
        if item in self.active:
            self.active.remove(item)
        self.recent.reject()
        self.total.reject()

    def reset_recent(self) -> None:
        """Reset recent counters."""
        self.recent.reset()

    def format_status(self, name: str) -> str:
        """Format status string with input>output when different, relative paths for active items."""
        q: int = len(self.queue)
        
        # Include pending_queue in queue count for accurate display
        if hasattr(self, 'pending_queue'):
            q += len(self.pending_queue)
        
        a: int = len(self.active)
        m: int = self.maximum
        
        # Format recent and total using TaskStats.format()
        recent_fmt: str = self.recent.format()
        total_fmt: str = self.total.format()
        
        # Format active items with relative paths
        active_str: str = ""
        if a > 0 and self.active:
            active_items: List[str] = []
            for item in self.active[:2]:
                # Extract path from item (could be string or tuple)
                item_str: str
                if isinstance(item, tuple):
                    # For tasks like Download/LLM that pass (path, handle/content)
                    item_str = str(item[0])
                else:
                    item_str = str(item)
                
                # Relativize path if possible (handle both regular and UNC paths)
                if self.input_dir and isinstance(item_str, str):
                    try:
                        # Check if path starts with input_dir (works for UNC and regular paths)
                        if item_str.startswith(self.input_dir):
                            item_str = os.path.relpath(item_str, self.input_dir)
                        elif os.path.isabs(item_str):
                            # Try relpath anyway, fallback to basename
                            item_str = os.path.relpath(item_str, self.input_dir)
                    except (ValueError, TypeError):
                        # Different drives or other issues - use basename as fallback
                        item_str = os.path.basename(item_str)
                active_items.append(item_str)
            active_str = f" ({', '.join(active_items)})"
            if a > 2:
                active_str = f" ({', '.join(active_items)}, ...)"
        
        return f"{name}: {q}Q->{a}A/{m}M->{recent_fmt}/{total_fmt}{active_str}"
