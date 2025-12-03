import os


class Task:
    def __init__(self, maximum=1, input_dir=None):
        self.queue = []          # items waiting
        self.active = []         # items processing
        self.maximum = maximum   # max active allowed
        self.input_dir = input_dir  # for relative path display

        # recent counters
        self.recent_done = 0
        self.recent_failed = 0
        self.recent_input = 0
        self.recent_output = 0
        self.recent_rejected = 0

        # total counters
        self.total_done = 0
        self.total_failed = 0
        self.total_input = 0
        self.total_output = 0
        self.total_rejected = 0

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
        self.recent_done += 1
        self.total_done += 1
        self.recent_input += 1
        self.total_input += 1
        self.recent_output += output_count
        self.total_output += output_count

    def fail(self, item):
        """Mark as failed."""
        if item in self.active:
            self.active.remove(item)
        self.recent_failed += 1
        self.total_failed += 1
        self.recent_input += 1
        self.total_input += 1

    def reject(self, item):
        """Mark as rejected (e.g., skipped)."""
        if item in self.active:
            self.active.remove(item)
        self.recent_rejected += 1
        self.total_rejected += 1
        self.recent_input += 1
        self.total_input += 1

    def reset_recent(self):
        """Reset recent counters."""
        self.recent_done = 0
        self.recent_failed = 0
        self.recent_input = 0
        self.recent_output = 0
        self.recent_rejected = 0

    def format_status(self, name):
        """Format status string with input>output when different, relative paths for active items."""
        q = len(self.queue)
        
        # Include pending_queue in queue count for accurate display
        if hasattr(self, 'pending_queue'):
            q += len(self.pending_queue)
        
        a = len(self.active)
        m = self.maximum
        rf = self.recent_failed
        ri = self.recent_input
        ro = self.recent_output
        rr = self.recent_rejected
        recent_fmt = ""
        
        if ri != ro:
            recent_fmt = f"{ri}>"

        if rf > 0:
            recent_fmt = f"{recent_fmt}{rf}F"
        
        if rr > 0:
            recent_fmt = f"{recent_fmt}{rr}R"

        recent_fmt = f"{recent_fmt}{ro}D"
        
        tf = self.total_failed
        ti = self.total_input
        to = self.total_output
        tr = self.total_rejected
        total_fmt = ""
        
        if ti != to:
            total_fmt = f"{ti}>"
        
        if tf > 0:
            total_fmt = f"{total_fmt}{tf}F"
        
        if tr > 0:
            total_fmt = f"{total_fmt}{tr}R"

        total_fmt = f"{total_fmt}{to}D"
        
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
