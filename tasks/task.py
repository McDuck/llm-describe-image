import os


class TaskStats:
    """Statistics for a task including input/output counts, failures, and rejections."""
    def __init__(self):
        self.done = 0
        self.failed = 0
        self.input = 0
        self.output = 0
        self.rejected = 0
    
    def reset(self):
        """Reset all counters to zero."""
        self.done = 0
        self.failed = 0
        self.input = 0
        self.output = 0
        self.rejected = 0
    
    def finish(self, output_count=1):
        """Record a finished item with output count."""
        self.done += 1
        self.input += 1
        self.output += output_count
    
    def fail(self):
        """Record a failed item."""
        self.failed += 1
        self.input += 1
    
    def reject(self):
        """Record a rejected item."""
        self.rejected += 1
        self.input += 1
    
    def format(self):
        """Format stats as string: [input>][failed F][rejected R]output D"""
        fmt = ""
        if self.input != self.output:
            fmt = f"{self.input}>"
        if self.failed > 0:
            fmt = f"{fmt}{self.failed}F"
        if self.rejected > 0:
            fmt = f"{fmt}{self.rejected}R"
        fmt = f"{fmt}{self.output}D"
        return fmt


class Task:
    def __init__(self, maximum=1, input_dir=None):
        self.queue = []          # items waiting
        self.active = []         # items processing
        self.maximum = maximum   # max active allowed
        self.input_dir = input_dir  # for relative path display

        # Stats objects
        self.recent = TaskStats()
        self.total = TaskStats()

    # --- Queue Management ----------------------------------------------------

    def add(self, item):
        """Add an item to the queue."""
        self.queue.append(item)

    def start_next(self, next_task=None, backpressure_multiplier=2.0):
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

    def finish(self, item, output_count=1):
        """Mark as done with output count."""
        if item in self.active:
            self.active.remove(item)
        self.recent.finish(output_count)
        self.total.finish(output_count)

    def fail(self, item):
        """Mark as failed."""
        if item in self.active:
            self.active.remove(item)
        self.recent.fail()
        self.total.fail()

    def reject(self, item):
        """Mark as rejected (e.g., skipped)."""
        if item in self.active:
            self.active.remove(item)
        self.recent.reject()
        self.total.reject()

    def reset_recent(self):
        """Reset recent counters."""
        self.recent.reset()

    def format_status(self, name):
        """Format status string with input>output when different, relative paths for active items."""
        q = len(self.queue)
        
        # Include pending_queue in queue count for accurate display
        if hasattr(self, 'pending_queue'):
            q += len(self.pending_queue)
        
        a = len(self.active)
        m = self.maximum
        
        # Format recent and total using TaskStats.format()
        recent_fmt = self.recent.format()
        total_fmt = self.total.format()
        
        # Format active items with relative paths
        active_str = ""
        if a > 0 and self.active:
            active_items = []
            for item in self.active[:2]:
                # Extract path from item (could be string or tuple)
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
