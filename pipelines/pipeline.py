"""Base class for image processing pipelines."""

import abc
import os
import sys
import time
import threading
from typing import Dict, List, Optional, Any, Tuple, Callable
from types import ModuleType
from dataclasses import dataclass

from tasks.task import Task


@dataclass
class TaskDefinition:
    """Definition of a task in the pipeline."""
    name: str  # Display name for status
    task_dir: str  # Directory containing task.py (e.g., "discover")
    task_class: str  # Class name (e.g., "DiscoverTask")
    num_workers: int  # Number of worker threads
    next_task: Optional[str] = None  # Name of next task in chain
    transform: Optional[Callable[[Any], Any]] = None  # Transform result before passing to next
    check_rejection: Optional[Callable[[Any], bool]] = None  # Return True to reject item
    has_pending_queue: bool = False  # For tasks with pending queue
    kwargs: Optional[Dict[str, Any]] = None  # Additional kwargs for task constructor


class Pipeline(abc.ABC):
    """Abstract base class for image processing pipelines."""
    
    def __init__(
        self,
        input_dir: str,
        output_dir: str,
        model_name: str,
        sort_order: str,
        retry_failed: bool,
        args: Any,
        stop_event: threading.Event,
        status_lock: threading.Lock,
        task_completed_items: Dict[str, List[Tuple[Any, Optional[List[Any]]]]],
        backpressure_multiplier: float,
        image_extensions: set
    ):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.model_name = model_name
        self.sort_order = sort_order
        self.retry_failed = retry_failed
        self.args = args
        self.stop_event = stop_event
        self.status_lock = status_lock
        self.task_completed_items = task_completed_items
        self.backpressure_multiplier = backpressure_multiplier
        self.image_extensions = image_extensions
        
        self.tasks: Dict[str, Task] = {}
        self.threads: List[threading.Thread] = []
        self.status_thread: Optional[threading.Thread] = None
    
    @abc.abstractmethod
    def get_task_definitions(self) -> List[TaskDefinition]:
        """Return list of task definitions for this pipeline."""
        raise NotImplementedError
    
    def load_prompt(self, default_prompt: str, env_var: str = "PROMPT") -> str:
        """Load prompt from args, file, or environment/default. Also sets self.prompt_source."""
        prompt_text: str = self.args.prompt or os.getenv(env_var, default_prompt)
        if self.args.prompt_file:
            with open(self.args.prompt_file, "r", encoding="utf-8") as f:
                prompt_text = f.read().strip()
            self.prompt_source = 'file'
        elif prompt_text.startswith("@"):
            with open(prompt_text[1:], "r", encoding="utf-8") as f:
                prompt_text = f.read().strip()
            self.prompt_source = 'file'
        else:
            self.prompt_source = 'inline' if self.args.prompt or os.getenv(env_var) else 'config'
        return prompt_text
    
    def print_base_configuration(self, model_name: str, prompt_source: str) -> None:
        """Print common configuration (model and prompt source)."""
        print(f"Using model: {model_name}")
        print(f"Prompt source: {prompt_source}")
    
    def print_thread_counts(self, task_defs: List[TaskDefinition]) -> None:
        """Print thread counts for all tasks."""
        thread_info = ", ".join([f"{td.name.lower()}={td.num_workers}" for td in task_defs if td.num_workers > 1])
        if thread_info:
            print(f"Threads: {thread_info}")
    
    @abc.abstractmethod
    def print_configuration(self) -> None:
        """Print pipeline configuration."""
        raise NotImplementedError
    
    def load_task(self, task_dir: str) -> Optional[ModuleType]:
        """Load task.py from a task directory."""
        task_file = os.path.join(task_dir, "task.py")
        if not os.path.exists(task_file):
            return None
        
        import importlib.util
        spec = importlib.util.spec_from_file_location(f"tasks.{os.path.basename(task_dir)}.task", task_file)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    
    def create_tasks(self) -> Dict[str, Task]:
        """Create tasks from definitions (called by run())."""
        tasks: Dict[str, Task] = {}
        task_defs = self.get_task_definitions()
        # Get tasks directory relative to the project root (parent of pipelines directory)
        tasks_dir: str = os.path.join(os.path.dirname(__file__), "..", "tasks")
        
        for task_def in task_defs:
            # Load task module
            module = self.load_task(os.path.join(tasks_dir, task_def.task_dir))
            if not module:
                raise Exception(f"Failed to load task: {task_def.task_dir}")
            
            # Get task class
            task_class = getattr(module, task_def.task_class)
            
            # Prepare kwargs
            kwargs = task_def.kwargs or {}
            
            # Create task instance
            task_instance = task_class(
                maximum=task_def.num_workers,
                **kwargs
            )
            
            tasks[task_def.name] = task_instance
        
        # Initialize first task (usually discover)
        if task_defs:
            first_task_name = task_defs[0].name
            if first_task_name in tasks and hasattr(tasks[first_task_name], 'add'):
                tasks[first_task_name].add(self.input_dir)
        
        return tasks
    
    def start_workers(self, tasks: Dict[str, Task]) -> List[threading.Thread]:
        """Start worker threads from definitions (called by run())."""
        threads: List[threading.Thread] = []
        task_defs = self.get_task_definitions()
        
        for task_def in task_defs:
            task = tasks[task_def.name]
            next_task = tasks.get(task_def.next_task) if task_def.next_task else None
            
            for _ in range(task_def.num_workers):
                t = threading.Thread(
                    name=task_def.name,
                    target=self.worker_thread,
                    args=(
                        task,
                        next_task,
                        task_def.transform,
                        task_def.check_rejection,
                        task_def.has_pending_queue
                    ),
                    daemon=False
                )
                t.start()
                threads.append(t)
        
        return threads
    
    def run(self) -> None:
        """Execute the pipeline."""
        # Print configuration
        self.print_configuration()
        
        # Create tasks
        self.tasks = self.create_tasks()
        
        # Start status printer
        from llm_describe_directory import status_printer
        self.status_thread = threading.Thread(
            name="Status",
            target=status_printer,
            args=(self.tasks, self.stop_event, self.status_lock, self.task_completed_items, self.args.status_interval),
            daemon=False
        )
        self.status_thread.start()
        
        # Start worker threads
        self.threads = self.start_workers(self.tasks)
        
        # Wait for completion
        self.wait_for_completion()
        
        # Print final stats
        try:
            from llm_describe_directory import format_and_print_status
            print("\nDone. Final update:")
            format_and_print_status(self.tasks, include_verbose=True)
        except Exception:
            pass  # Ignore print errors during shutdown
    
    def wait_for_completion(self) -> None:
        """Wait for all tasks to complete."""
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
        
        # Signal all threads to stop
        self.stop_event.set()
        
        # Wait for all worker threads to finish
        for t in self.threads:
            if t.is_alive():
                t.join(timeout=5.0)
        
        # Wait for status thread
        if self.status_thread and self.status_thread.is_alive():
            self.status_thread.join(timeout=2.0)
        
        # Final check for any remaining threads
        alive_threads = [t for t in self.threads if t.is_alive()]
        if alive_threads:
            try:
                print("Waiting for threads to finish...", ", ".join([t.name for t in alive_threads]))
            except:
                pass  # Ignore print errors during shutdown
            for t in alive_threads:
                t.join(timeout=5.0)
    
    def worker_thread(
        self,
        task: Task,
        next_task: Optional[Task] = None,
        transform: Optional[Callable[[Any], Any]] = None,
        check_rejection: Optional[Callable[[Any], bool]] = None,
        has_pending_queue: bool = False
    ) -> None:
        """Generic worker thread that processes items from a task queue."""
        from llm_describe_directory import worker_thread as base_worker_thread
        base_worker_thread(
            task,
            self.stop_event,
            self.status_lock,
            self.task_completed_items,
            self.backpressure_multiplier,
            next_task,
            transform,
            check_rejection,
            has_pending_queue,
            self.tasks
        )
