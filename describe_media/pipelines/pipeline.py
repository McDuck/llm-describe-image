"""Base Pipeline class for orchestrating task workflows."""

import os
import time
import threading
import signal
import importlib.util
from datetime import datetime
from types import ModuleType
from typing import Dict, Optional, List, Callable, Tuple, Any, Set
from describe_media.config_loader import DEFAULT_MAX_CONCURRENT_TASKS
from describe_media.tasks.task import Task


class Pipeline:
    """Base class for pipelines that orchestrate multiple tasks."""
    
    def __init__(self, name: str, description: str) -> None:
        """
        Initialize pipeline.
        
        Args:
            name: Pipeline name (e.g., 'describe')
            description: Pipeline description
        """
        self.name: str = name
        self.description: str = description
        self.tasks: Dict[str, Task] = {}
        
        # Set tasks directory (pipelines/X/pipeline.py â†’ tasks/)
        self.tasks_dir: str = os.path.join(os.path.dirname(__file__), "..", "tasks")
        
        # Pipeline state
        self.stop_event: threading.Event = threading.Event()
        self.status_lock: threading.Lock = threading.Lock()
        self.task_completed_items: Dict[str, List[Tuple[Any, Optional[List[Any]]]]] = {}
        
        # Configuration
        self.verbose: bool = False
        self.backpressure_multiplier: float = 2.0
        # Per-stage thread settings cannot prevent separate stages from running
        # together, so this is enforced by the scheduler below.
        self.max_concurrent_tasks: int = DEFAULT_MAX_CONCURRENT_TASKS
        self._round_robin_cursor: Dict[int, int] = {}
    
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
        Load task modules and create task instances from self.PIPELINE_CONFIG or self.TASK_CONFIG.
        Supports both merged PIPELINE_CONFIG and separate TASK_CONFIG + THREAD_CONFIG.
        """
        # Use PIPELINE_CONFIG if available, otherwise fall back to TASK_CONFIG
        task_configs = getattr(self, 'PIPELINE_CONFIG', None) or getattr(self, 'TASK_CONFIG', None)
        
        if not task_configs:
            raise RuntimeError(f"{self.__class__.__name__} must define PIPELINE_CONFIG or TASK_CONFIG")
        
        if not self.tasks_dir:
            raise RuntimeError(f"{self.__class__.__name__} must set self.tasks_dir")
        
        for config in task_configs:
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
        
        spec = importlib.util.spec_from_file_location(f"describe_media.tasks.{os.path.basename(task_dir)}.task", task_file)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    
    def _format_and_print_status(self, include_verbose: bool = False) -> None:
        """Format and print the status line with optional verbose output."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Build status line using each task's format_status method
        parts: List[str] = []
        for name, task in self.tasks.items():
            status_str = self._format_task_status(name, task)
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
                    
                    # Extract a stable display path. Extracted frames are stored
                    # outside INPUT_DIR, so prefer their logical output path.
                    item_path: Any = item
                    if isinstance(item, tuple):
                        item_path = item[0]
                        if len(item) >= 2 and isinstance(item[1], dict):
                            item_path = item[1].get("_output_relative_path", item_path)
                    
                    # Format outputs if present
                    if outputs:
                        output_names: List[str] = []
                        for out in outputs[:3]:
                            output_path: Any = out
                            if isinstance(out, tuple):
                                output_path = out[0]
                                if len(out) >= 2 and isinstance(out[1], dict):
                                    output_path = out[1].get("_output_relative_path", output_path)
                            if isinstance(output_path, str):
                                output_names.append(output_path.split("/")[-1].split("\\")[-1])
                        
                        if len(outputs) > 3:
                            output_str = ", ".join(output_names) + ", ..."
                        else:
                            output_str = ", ".join(output_names)
                        
                        count_prefix = f"{len(outputs)} output{'s' if len(outputs) != 1 else ''}: "
                        verbose_parts.append(f"{item_path} -> {count_prefix}{output_str}")
                    else:
                        verbose_parts.append(f"{item_path} -> (no output)")
            
            if verbose_parts:
                status_line += "\n  " + "\n  ".join(verbose_parts)
        
        print(status_line)

    def _format_task_status(self, name: str, task: Task) -> str:
        """Use compact artifact-oriented progress for the integrated graph."""
        if self.name != "describe" or name in {"Discover", "LLM"}:
            return task.format_status(name)
        output_symbols = {
            "ExtractVideo": "f",
            "ImageRouter": "i",
            "Metadata": "m",
            "Resize": "p",
            "Recognition": "r",
            "Geolocate": "g",
            "Enhance": "e",
            "Shortcut": "s",
            "FixJPEG": "j",
            "VideoError": "E",
        }
        fields = [f"{len(task.queue)}Q"]
        if task.active:
            fields.append(f"{len(task.active)}A")
        if task.total.failed:
            fields.append(f"{task.total.failed}F")
        return f"{name}: {' '.join(fields)} ->{task.total.output}{output_symbols.get(name, 'D')}"
    
    def _worker_thread(
        self,
        task: Task,
        next_task: Optional[Task] = None,
        next_tasks: Optional[List[Task]] = None,
        transform: Optional[Callable[[Any], Any]] = None,
        check_rejection: Optional[Callable[[Any], bool]] = None,
        has_pending_queue: bool = False,
        route: Optional[Callable[[Any], Optional[str]]] = None,
        error_task: Optional[Task] = None,
    ) -> None:
        """Execute a worker thread for a task."""
        loaded = False
        try:
            while not self.stop_event.is_set():
                # Try to start next item (check downstream capacity with backpressure)
                item = task.start_next(next_task, self.backpressure_multiplier)
                if item is None:
                    time.sleep(0.1)
                    continue

                if not loaded:
                    try:
                        task.load()
                        loaded = True
                    except Exception as e:
                        print(f"Error initializing {task.__class__.__name__}: {e}")
                        task.fail(item)
                        self.stop_event.set()
                        return
                
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
                        
                        # Queue child directories immediately. Deferring them
                        # until the next status update leaves a completion
                        # window in which recursive discovery appears empty.
                        if pending_items and hasattr(task, 'pending_queue'):
                            with self.status_lock:
                                for pending_item in pending_items:
                                    task.add(pending_item)
                    
                    # Pass every successful result onward, including a cached
                    # result. Each downstream stage owns its own output/error
                    # gate, so a cache hit here must never terminate the graph.
                    if (next_task is not None or next_tasks or route is not None) and not is_rejected:
                        results = result if isinstance(result, list) else [result]
                        for output in results:
                            if output is None:
                                continue
                            targets: List[Task] = list(next_tasks or ([] if next_task is None else [next_task]))
                            if route is not None:
                                target_names = route(output)
                                if isinstance(target_names, str):
                                    target_names = [target_names]
                                targets = [
                                    target
                                    for target_name in (target_names or [])
                                    for target in [self.get_task(target_name)]
                                    if target is not None
                                ]
                            outgoing = transform(output) if transform else output
                            if outgoing is None:
                                continue
                            for target in targets:
                                target.add(outgoing)

                    if not is_rejected:
                        # Finish only after every downstream and pending item
                        # has been queued; otherwise the completion monitor can
                        # stop the pipeline in the hand-off gap.
                        task.finish(item, output_count)

                        # Track completed item for status line verbose output.
                        if self.verbose:
                            task_name = task.__class__.__name__.replace('Task', '')
                            with self.status_lock:
                                if task_name not in self.task_completed_items:
                                    self.task_completed_items[task_name] = []

                                if pending_items:
                                    self.task_completed_items[task_name].append((item, pending_items))
                                elif isinstance(result, list) and result:
                                    self.task_completed_items[task_name].append((item, result))
                                elif result is not None:
                                    self.task_completed_items[task_name].append((item, [result]))
                                else:
                                    self.task_completed_items[task_name].append((item, None))

                            self._format_and_print_status(include_verbose=True)
                            self.task_completed_items.clear()
                            for t in self.tasks.values():
                                t.reset_recent()
                            
                except Exception as e:
                    # Route errors explicitly when a graph branch needs its own
                    # retry marker; otherwise preserve the existing WriteTask path.
                    target = error_task or next_task
                    if target is not None:
                        # Extract input path from item
                        if isinstance(item, tuple) and len(item) >= 1:
                            input_path = item[0]
                            metadata = item[1] if len(item) >= 2 and isinstance(item[1], dict) else None
                            target.add((input_path, e, metadata) if metadata is not None and error_task is None else (input_path, e))
                        elif isinstance(item, str):
                            target.add((item, e))
                    
                    task.fail(item)
        finally:
            # Unload resources at thread end
            if loaded:
                try:
                    task.unload()
                except Exception:
                    pass
    
    def _process_item(
        self,
        task: Task,
        item: Any,
        next_task: Optional[Task] = None,
        next_tasks: Optional[List[Task]] = None,
        transform: Optional[Callable[[Any], Any]] = None,
        check_rejection: Optional[Callable[[Any], bool]] = None,
        has_pending_queue: bool = False,
        route: Optional[Callable[[Any], Optional[str]]] = None,
        error_task: Optional[Task] = None,
    ) -> None:
        """Execute one item and enqueue all work it makes ready."""
        try:
            result = task.execute(item)
            is_rejected = check_rejection(result) if check_rejection else False
            if is_rejected:
                task.reject(item)
                return

            output_count = 0
            pending_items: List[Any] = []
            if has_pending_queue and isinstance(result, tuple) and len(result) == 2:
                result, pending_items = result
                output_count = len(result) if isinstance(result, list) else (1 if result else 0)
                output_count += len(pending_items) if isinstance(pending_items, list) else 0
            elif isinstance(result, list):
                output_count = len(result)
            elif result is not None:
                output_count = 1

            if pending_items and hasattr(task, "pending_queue"):
                with self.status_lock:
                    for pending_item in pending_items:
                        task.add(pending_item)

            if next_task is not None or next_tasks or route is not None:
                for output in result if isinstance(result, list) else [result]:
                    if output is None:
                        continue
                    targets: List[Task] = list(next_tasks or ([] if next_task is None else [next_task]))
                    if route is not None:
                        target_names = route(output)
                        if isinstance(target_names, str):
                            target_names = [target_names]
                        targets = [
                            target
                            for target_name in (target_names or [])
                            for target in [self.get_task(target_name)]
                            if target is not None
                        ]
                    outgoing = transform(output) if transform else output
                    if outgoing is not None:
                        for target in targets:
                            target.add(outgoing)

            task.finish(item, output_count)
            if self.verbose:
                task_name = task.__class__.__name__.replace("Task", "")
                verbose_output = pending_items or (
                    result if isinstance(result, list) and result else [result] if result is not None else None
                )
                with self.status_lock:
                    self.task_completed_items.setdefault(task_name, []).append((item, verbose_output))
                self._format_and_print_status(include_verbose=True)
                self.task_completed_items.clear()
                for pipeline_task in self.tasks.values():
                    pipeline_task.reset_recent()
        except Exception as e:
            target = error_task or next_task
            if target is not None:
                if isinstance(item, tuple) and item:
                    input_path = item[0]
                    metadata = item[1] if len(item) >= 2 and isinstance(item[1], dict) else None
                    target.add((input_path, e, metadata) if metadata is not None and error_task is None else (input_path, e))
                elif isinstance(item, str):
                    target.add((item, e))
            task.fail(item)

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
    
    def _thread_configs(self) -> List[Dict[str, Any]]:
        """Return the worker configuration shared by both schedulers."""
        configs = getattr(self, "PIPELINE_CONFIG", None) or getattr(self, "THREAD_CONFIG", None)
        if not configs:
            raise RuntimeError(f"{self.__class__.__name__} must define PIPELINE_CONFIG or THREAD_CONFIG")
        return configs

    def _config_successors(self, config: Dict[str, Any]) -> List[str]:
        """Return the static destinations used to rank a graph stage."""
        destinations = list(config.get("next_tasks", []))
        if config.get("next_task"):
            destinations.append(config["next_task"])
        # Routed stages declare every possible target. Dynamic routing still
        # decides the actual destination at execution time.
        destinations.extend(config.get("route_targets", []))
        return destinations

    def _task_distances_to_output(self, configs: List[Dict[str, Any]]) -> Dict[str, int]:
        """Return scheduler priorities; smaller values are further downstream."""
        edges = {config["name"]: self._config_successors(config) for config in configs}
        visiting: Set[str] = set()
        cache: Dict[str, int] = {}

        def distance(name: str) -> int:
            if name in cache:
                return cache[name]
            if name in visiting:
                return 0
            visiting.add(name)
            children = [child for child in edges.get(name, []) if child in edges]
            value = 0 if not children else 1 + min(distance(child) for child in children)
            visiting.remove(name)
            cache[name] = value
            return value

        # A routed stage can serve both a terminal side branch and a much deeper
        # critical branch. An explicit priority prevents the side branch from
        # starving prerequisite work for a downstream fan-in.
        return {
            config["name"]: config.get("priority", distance(config["name"]))
            for config in configs
        }

    def _next_serial_item(self, configs: List[Dict[str, Any]], distances: Dict[str, int]) -> Optional[Tuple[Dict[str, Any], Any]]:
        """Choose one ready item: downstream first, round-robin on equal depth."""
        ready: List[Tuple[int, Dict[str, Any]]] = []
        for index, config in enumerate(configs):
            task = self.get_task(config["task"])
            if task is not None and task.queue and len(task.active) < task.maximum:
                ready.append((index, config))
        if not ready:
            return None

        nearest_output = min(distances.get(config["name"], 0) for _, config in ready)
        candidates = [(index, config) for index, config in ready if distances.get(config["name"], 0) == nearest_output]
        cursor = self._round_robin_cursor.get(nearest_output, 0)
        selected_index, selected = next(
            ((index, config) for index, config in candidates if index >= cursor),
            candidates[0],
        )
        self._round_robin_cursor[nearest_output] = (selected_index + 1) % len(configs)

        task = self.get_task(selected["task"])
        next_task = self.get_task(selected["next_task"]) if selected.get("next_task") else None
        item = task.start_next(next_task, self.backpressure_multiplier) if task is not None else None
        return (selected, item) if item is not None else None

    def _serial_worker(self) -> None:
        """Run graph work with exactly one executing pipeline task."""
        configs = self._thread_configs()
        distances = self._task_distances_to_output(configs)
        loaded: Dict[str, Task] = {}
        try:
            while not self.stop_event.is_set():
                scheduled = self._next_serial_item(configs, distances)
                if scheduled is None:
                    time.sleep(0.05)
                    continue

                config, item = scheduled
                task = self.get_task(config["task"])
                if task is None:
                    continue
                if config["task"] not in loaded:
                    try:
                        task.load()
                        loaded[config["task"]] = task
                    except Exception as e:
                        print(f"Error initializing {task.__class__.__name__}: {e}")
                        task.fail(item)
                        self.stop_event.set()
                        return

                next_task = self.get_task(config["next_task"]) if config.get("next_task") else None
                next_tasks = [
                    downstream
                    for name in config.get("next_tasks", [])
                    for downstream in [self.get_task(name)]
                    if downstream is not None
                ]
                error_task = self.get_task(config["error_task"]) if config.get("error_task") else None
                self._process_item(
                    task, item, next_task, next_tasks, config.get("transform"),
                    config.get("check_rejection"), config.get("has_pending_queue", False),
                    config.get("route"), error_task,
                )
        finally:
            for task in reversed(list(loaded.values())):
                try:
                    task.unload()
                except Exception:
                    pass

    def _create_worker_threads(self) -> List[threading.Thread]:
        """
        Create and start worker threads from self.PIPELINE_CONFIG or self.THREAD_CONFIG.
        Supports both merged PIPELINE_CONFIG and separate TASK_CONFIG + THREAD_CONFIG.
        """
        thread_configs = self._thread_configs()

        if self.max_concurrent_tasks == 1:
            scheduler = threading.Thread(name="Scheduler", target=self._serial_worker, daemon=True)
            scheduler.start()
            return [scheduler]
        
        threads: List[threading.Thread] = []
        
        for config in thread_configs:
            # Get current task
            current_task = self.get_task(config["task"])
            
            # Get next task (if any)
            next_task = self.get_task(config["next_task"]) if config.get("next_task") else None
            next_tasks = [
                task
                for task_name in config.get("next_tasks", [])
                for task in [self.get_task(task_name)]
                if task is not None
            ]
            error_task = self.get_task(config["error_task"]) if config.get("error_task") else None
            
            # Determine number of threads
            if "num_threads" in config:
                num_threads = config["num_threads"]
            else:
                num_threads = getattr(self, config["num_threads_getter"])
            
            # Get optional functions
            transform = config.get("transform")
            check_rejection = config.get("check_rejection")
            has_pending_queue = config.get("has_pending_queue", False)
            route = config.get("route")
            
            # Create threads for this task
            for _ in range(num_threads):
                t = threading.Thread(
                    name=config["task"],
                    target=self._worker_thread,
                    args=(current_task, next_task, next_tasks, transform, check_rejection, has_pending_queue, route, error_task),
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
        self.input_dir = os.path.abspath(input_dir)
        self.output_dir = os.path.abspath(output_dir or input_dir)
        self.verbose = verbose
        
        # Load tasks using generic loader
        self._load_tasks_from_config()
        
        # Add initial items to first task (get first task from either PIPELINE_CONFIG or TASK_CONFIG)
        config = getattr(self, 'PIPELINE_CONFIG', None) or getattr(self, 'TASK_CONFIG', None)
        first_task = self.get_task(config[0]["name"])
        discovery_dir = self.input_dir
        subdirectory = kwargs.get("subdirectory")
        if subdirectory:
            candidate = os.path.abspath(os.path.join(self.input_dir, str(subdirectory)))
            try:
                is_within_input = os.path.commonpath([self.input_dir, candidate]) == self.input_dir
            except ValueError:
                is_within_input = False
            if not is_within_input:
                raise ValueError("Subdirectory must remain below INPUT_DIR")
            if not os.path.isdir(candidate):
                raise ValueError(f"Subdirectory does not exist: {candidate}")
            discovery_dir = candidate
        first_task.add(discovery_dir)
        
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
                self.stop_event.set()
                break
            
            time.sleep(0.5)

        self.stop_event.set()
        
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
