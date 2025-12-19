"""Base Pipeline class for orchestrating task workflows."""

import os
import time
import threading
import signal
import importlib.util
from datetime import datetime
from types import ModuleType
from typing import Dict, Optional, List, Callable, Tuple, Any
from tasks.task import Task


class Pipeline:
    """Base class for pipelines that orchestrate multiple tasks."""
    
    def __init__(self, name: str, description: str) -> None:
        """
        Initialize pipeline.
        
        Args:
            name: Pipeline name (e.g., 'describe-image')
            description: Pipeline description
        """
        self.name: str = name
        self.description: str = description
        self.tasks: Dict[str, Task] = {}
        
        # Set tasks directory (pipelines/X/pipeline.py → tasks/)
        self.tasks_dir: str = os.path.join(os.path.dirname(__file__), "..", "tasks")
        
        # Pipeline state
        self.stop_event: threading.Event = threading.Event()
        self.status_lock: threading.Lock = threading.Lock()
        self.task_completed_items: Dict[str, List[Tuple[Any, Optional[List[Any]]]]] = {}
        
        # Configuration
        self.verbose: bool = False
        self.backpressure_multiplier: float = 2.0
    
    def add_task(self, name: str, task: Task) -> None:
        """Add a task to the pipeline."""
        self.tasks[name] = task
    
    def get_task(self, name: str) -> Optional[Task]:
        """Get a task by name."""
        return self.tasks.get(name)
    
    def get_all_tasks(self) -> Dict[str, Task]:
        """Get all tasks in the pipeline."""
        return self.tasks
    
    def _load_tasks_from_config(self) -> None:
        """
        Load task modules and create task instances from self.TASK_CONFIG.
        Subclasses must define TASK_CONFIG and set self.tasks_dir.
        """
        if not hasattr(self, 'TASK_CONFIG'):
            raise RuntimeError(f"{self.__class__.__name__} must define TASK_CONFIG")
        
        if not self.tasks_dir:
            raise RuntimeError(f"{self.__class__.__name__} must set self.tasks_dir")
        
        for config in self.TASK_CONFIG:
            # Load task module
            task_module = self._load_task_module(os.path.join(self.tasks_dir, config["dir"]))
            if not task_module:
                raise RuntimeError(f"Failed to load task module from {config['dir']}")
            
            # Get task class
            task_class = getattr(task_module, config["class_name"])
            
            # Build kwargs
            kwargs = config["kwargs_builder"](self)
            
            # Instantiate task
            task_instance = task_class(**kwargs)
            
            # Add to pipeline
            self.add_task(config["name"], task_instance)
    
    @staticmethod
    def _load_task_module(task_dir: str) -> Optional[ModuleType]:
        """Load task.py from a task directory."""
        task_file = os.path.join(task_dir, "task.py")
        if not os.path.exists(task_file):
            return None
        
        spec = importlib.util.spec_from_file_location(f"tasks.{os.path.basename(task_dir)}.task", task_file)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    
    def _format_and_print_status(self, include_verbose: bool = False) -> None:
        """Format and print the status line with optional verbose output."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Build status line using each task's format_status method
        parts: List[str] = []
        for name, task in self.tasks.items():
            status_str = task.format_status(name)
            parts.append(status_str)
        
        # Print combined status
        status_line = f"[{timestamp}] " + " | ".join(parts)
        
        # Append verbose output if enabled
        if include_verbose and self.verbose and self.task_completed_items:
            verbose_parts: List[str] = []
            for task_name, items in self.task_completed_items.items():
                if items:
                    # Get the last completed item
                    item, outputs = items[-1]
                    
                    # Extract path from item (could be string or tuple)
                    item_path: Any
                    if isinstance(item, tuple):
                        item_path = item[0]
                    else:
                        item_path = item
                    
                    # Format outputs if present
                    if outputs:
                        output_names: List[str] = []
                        for out in outputs[:3]:
                            if isinstance(out, str):
                                output_names.append(out.split("/")[-1].split("\\")[-1])
                        
                        if len(outputs) > 3:
                            output_str = ", ".join(output_names) + ", ..."
                        else:
                            output_str = ", ".join(output_names)
                        
                        verbose_parts.append(f"{item_path} → {output_str}")
                    else:
                        verbose_parts.append(f"{item_path} → (no output)")
            
            if verbose_parts:
                status_line += "\n  " + "\n  ".join(verbose_parts)
        
        print(status_line)
    
    def _worker_thread(
        self,
        task: Task,
        next_task: Optional[Task] = None,
        transform: Optional[Callable[[Any], Any]] = None,
        check_rejection: Optional[Callable[[Any], bool]] = None,
        has_pending_queue: bool = False
    ) -> None:
        """Execute a worker thread for a task."""
        try:
            task.load()
            
            while not self.stop_event.is_set():
                # Try to start next item (check downstream capacity with backpressure)
                item = task.start_next(next_task, self.backpressure_multiplier)
                if item is None:
                    time.sleep(0.1)
                    continue
                
                try:
                    # Execute task
                    result = task.execute(item)
                    
                    # Check if result should be rejected
                    is_rejected = check_rejection(result) if check_rejection else False
                    
                    if is_rejected:
                        task.reject(item)
                    else:
                        # Calculate output count
                        output_count: int = 0
                        pending_items: List[Any] = []
                        
                        # Special handling for tasks with pending queue (e.g., DiscoverTask)
                        if has_pending_queue and isinstance(result, tuple) and len(result) == 2:
                            actual_result, pending_items = result
                            output_count = len(actual_result) if isinstance(actual_result, list) else (1 if actual_result else 0)
                            output_count += len(pending_items) if isinstance(pending_items, list) else 0
                            result = actual_result
                        else:
                            # Standard output counting
                            if isinstance(result, list):
                                output_count = len(result)
                            elif result is not None:
                                output_count = 1
                        
                        task.finish(item, output_count)
                        
                        # Track completed item for status line verbose output
                        if self.verbose:
                            task_name = task.__class__.__name__.replace('Task', '')
                            with self.status_lock:
                                if task_name not in self.task_completed_items:
                                    self.task_completed_items[task_name] = []
                                
                                # Store item with its outputs for verbose display in status
                                if pending_items:
                                    self.task_completed_items[task_name].append((item, pending_items))
                                elif isinstance(result, list) and result:
                                    self.task_completed_items[task_name].append((item, result))
                                elif result is not None:
                                    self.task_completed_items[task_name].append((item, [result]))
                                else:
                                    self.task_completed_items[task_name].append((item, None))
                            
                            # Print status immediately after completion
                            self._format_and_print_status(include_verbose=True)
                            # Clear completed items after printing
                            self.task_completed_items.clear()
                            # Reset recent counters after printing
                            for t in self.tasks.values():
                                t.reset_recent()
                        
                        # Store pending items to be added after status print
                        if pending_items and hasattr(task, 'pending_queue'):
                            with self.status_lock:
                                task.pending_queue.extend(pending_items)
                    
                    # Pass to next stage if configured
                    if next_task is not None and not is_rejected:
                        if transform:
                            result = transform(result)
                        
                        # Handle different result types
                        if isinstance(result, list):
                            for r in result:
                                next_task.add(r)
                        elif result is not None:
                            next_task.add(result)
                            
                except Exception as e:
                    # If there's a next task, pass the error to it (for WriteTask to handle)
                    if next_task is not None:
                        # Extract input path from item
                        if isinstance(item, tuple) and len(item) >= 1:
                            input_path = item[0]
                            next_task.add((input_path, e))
                    
                    task.fail(item)
        finally:
            # Unload resources at thread end
            try:
                task.unload()
            except Exception:
                pass
    
    def _status_printer(self, interval: float = 5.0) -> None:
        """Print periodic status updates."""
        while not self.stop_event.is_set():
            # Acquire lock to get consistent snapshot and flush pending queues
            with self.status_lock:
                # Print status with verbose output
                self._format_and_print_status(include_verbose=True)
                
                # Clear completed items after printing
                self.task_completed_items.clear()
                
                # Reset recent counters after formatting
                for task in self.tasks.values():
                    task.reset_recent()
                
                # Flush pending queue items AFTER printing status
                for name, task in self.tasks.items():
                    if hasattr(task, 'pending_queue') and task.pending_queue:
                        for item in task.pending_queue:
                            task.add(item)
                        task.pending_queue.clear()
            
            time.sleep(interval)
    
    def _create_worker_threads(self) -> List[threading.Thread]:
        """
        Create and start worker threads from self.THREAD_CONFIG.
        Subclasses must define THREAD_CONFIG.
        """
        if not hasattr(self, 'THREAD_CONFIG'):
            raise RuntimeError(f"{self.__class__.__name__} must define THREAD_CONFIG")
        
        threads: List[threading.Thread] = []
        
        for config in self.THREAD_CONFIG:
            # Get current task
            current_task = self.get_task(config["task"])
            
            # Get next task (if any)
            next_task = self.get_task(config["next_task"]) if config["next_task"] else None
            
            # Determine number of threads
            if "num_threads" in config:
                num_threads = config["num_threads"]
            else:
                num_threads = getattr(self, config["num_threads_getter"])
            
            # Get optional functions
            transform = config.get("transform")
            check_rejection = config.get("check_rejection")
            has_pending_queue = config.get("has_pending_queue", False)
            
            # Create threads for this task
            for _ in range(num_threads):
                t = threading.Thread(
                    name=config["task"],
                    target=self._worker_thread,
                    args=(current_task, next_task, transform, check_rejection, has_pending_queue),
                    daemon=True
                )
                t.start()
                threads.append(t)
        
        return threads
    
    def run(
        self,
        input_dir: str,
        output_dir: Optional[str] = None,
        verbose: bool = False,
        status_interval: float = 5.0,
        **kwargs
    ) -> None:
        """
        Run the pipeline.
        
        Args:
            input_dir: Input directory to process
            output_dir: Output directory for results (defaults to input_dir)
            verbose: Enable verbose output
            status_interval: Status update interval in seconds
        """
        if not hasattr(self, 'input_dir'):
            self.input_dir: Optional[str] = None
        if not hasattr(self, 'output_dir'):
            self.output_dir: Optional[str] = None
        
        # Set configuration
        self.input_dir = input_dir
        self.output_dir = output_dir or input_dir
        self.verbose = verbose
        
        # Load tasks using generic loader
        self._load_tasks_from_config()
        
        # Add initial items to first task
        first_task = self.get_task(self.TASK_CONFIG[0]["name"])
        first_task.add(self.input_dir)
        
        # Run the generic pipeline orchestration
        self._run_pipeline(status_interval=status_interval)
    
    def _run_pipeline(self, status_interval: float = 5.0) -> None:
        """
        Generic pipeline execution logic.
        Subclasses should call this after configuring tasks.
        """
        # Register signal handler
        def signal_handler(sig: int, frame: Any) -> None:
            print("\nReceived SIGINT. Stopping...")
            self.stop_event.set()
        
        signal.signal(signal.SIGINT, signal_handler)
        
        # Start status printer
        status_thread: threading.Thread = threading.Thread(
            name="Status",
            target=self._status_printer,
            args=(status_interval,),
            daemon=True
        )
        status_thread.start()
        
        # Start worker threads (configured by subclass)
        threads: List[threading.Thread] = self._create_worker_threads()
        
        # Wait for completion
        while not self.stop_event.is_set():
            # Check if all tasks are empty (including pending_queue)
            with self.status_lock:
                all_empty = all(
                    len(task.queue) == 0 and 
                    len(task.active) == 0 and
                    (not hasattr(task, 'pending_queue') or len(task.pending_queue) == 0)
                    for task in self.tasks.values()
                )
            
            if all_empty:
                break
            
            time.sleep(0.5)
        
        # Wait for threads to finish (with timeout)
        for t in threads:
            t.join(timeout=2.0)
        status_thread.join(timeout=1.0)

        threads_alive = True
        while threads_alive:
            threads_alive = False
            alive_threads = [t for t in threads if t.is_alive()]
            if alive_threads:
                print("Waiting for threads to finish...", ", ".join([t.name for t in alive_threads]))
                time.sleep(2.0)
        
        print("\nDone. Final update:")
        
        # Print final stats
        self._format_and_print_status(include_verbose=True)
