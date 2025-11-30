#!/usr/bin/env python3
"""
Task-based image description pipeline.
Uses modular Task classes for each stage of processing.
"""

import os
import sys
import time
import threading
import argparse
from datetime import datetime

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# Import tasks
from tasks.task import Task
import importlib.util

# Load task modules dynamically
def load_task(task_dir):
    """Load task.py from a task directory."""
    task_file = os.path.join(task_dir, "task.py")
    if not os.path.exists(task_file):
        return None
    
    spec = importlib.util.spec_from_file_location(f"tasks.{os.path.basename(task_dir)}.task", task_file)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# --- CONFIG ---
INPUT_DIR = os.getenv("INPUT_DIR")
OUTPUT_DIR = os.getenv("OUTPUT_DIR")
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp"}
MODEL_NAME = os.getenv("MODEL_NAME")
SORT_ORDER = os.getenv("SORT_ORDER", "natural-desc")
VERBOSE = False

# Thread counts
NUM_DISCOVER_THREADS = int(os.getenv("NUM_DISCOVER_THREADS", "100"))
NUM_SKIP_CHECKER_THREADS = int(os.getenv("NUM_SKIP_CHECKER_THREADS", "100"))
NUM_DOWNLOAD_THREADS = int(os.getenv("NUM_DOWNLOAD_THREADS", "2"))
NUM_LLM_THREADS = int(os.getenv("NUM_LLM_THREADS", "1"))
NUM_WRITE_THREADS = int(os.getenv("NUM_WRITE_THREADS", "1"))

# Backpressure multiplier: allow queue to grow to (max * multiplier) before stopping upstream work
BACKPRESSURE_MULTIPLIER = float(os.getenv("BACKPRESSURE_MULTIPLIER", "2.0"))

# Global state
stop_event = threading.Event()
sigint_pressed = False
status_lock = threading.Lock()  # Coordinate status printing and queue updates
task_completed_items = {}  # Track completed items per task for verbose output


def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully."""
    global sigint_pressed
    sigint_pressed = True
    stop_event.set()
    print("\n[Ctrl+C] Stopping...")


def format_and_print_status(tasks, include_verbose=False):
    """Format and print the status line with optional verbose output."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Build status line using each task's format_status method
    parts = []
    for name, task in tasks.items():
        status_str = task.format_status(name)
        parts.append(status_str)
    
    # Print combined status
    status_line = f"[{timestamp}] " + " | ".join(parts)
    
    # Append verbose output if enabled
    if include_verbose and VERBOSE and task_completed_items:
        verbose_parts = []
        for task_name, items in task_completed_items.items():
            if items:
                # Get the last completed item
                item, outputs = items[-1]
                
                # Extract path from item (could be string or tuple)
                if isinstance(item, tuple):
                    # For tasks like Download/LLM that pass (path, handle/content)
                    item_path = item[0]
                else:
                    item_path = item
                
                # Format item path relative to input dir
                if INPUT_DIR and isinstance(item_path, str):
                    try:
                        # Handle both regular and UNC paths
                        if item_path.startswith(INPUT_DIR):
                            rel_path = os.path.relpath(item_path, INPUT_DIR)
                        else:
                            rel_path = item_path
                    except ValueError:
                        # Different drives or UNC path issues
                        rel_path = item_path
                else:
                    rel_path = str(item_path)
                
                # Format outputs if present
                if outputs:
                    # Get output names (relative paths or basenames depending on task)
                    output_names = []
                    for out in outputs[:3]:  # Show first 3
                        if isinstance(out, str):
                            # For Write task, show relative path from OUTPUT_DIR
                            if task_name == "Write" and OUTPUT_DIR and out.startswith(OUTPUT_DIR):
                                try:
                                    output_names.append(os.path.relpath(out, OUTPUT_DIR))
                                except (ValueError, TypeError):
                                    output_names.append(os.path.basename(out))
                            else:
                                # For other tasks (Discover), show basename
                                output_names.append(os.path.basename(out))
                    
                    if len(outputs) > 3:
                        output_str = ", ".join(output_names) + ", ..."
                    elif output_names:
                        output_str = ", ".join(output_names)
                    else:
                        output_str = None
                    
                    if output_str:
                        verbose_parts.append(f"Done {task_name}: {rel_path} -> {output_str}")
                    else:
                        verbose_parts.append(f"Done {task_name}: {rel_path}")
                else:
                    verbose_parts.append(f"Done {task_name}: {rel_path}")
        
        if verbose_parts:
            status_line += " | " + ", ".join(verbose_parts)
    
    print(status_line)


def worker_thread(task, next_task=None, transform=None, check_rejection=None, has_pending_queue=False, tasks=None):
    """
    Generic worker thread that processes items from a task queue.
    
    Args:
        task: Task instance to process
        next_task: Optional next task to add results to
        transform: Optional function to transform results before adding to next_task
        check_rejection: Optional function to check if result should be rejected (returns True if rejected)
        has_pending_queue: If True, expects result to be (actual_result, pending_items) tuple
    """
    while not stop_event.is_set():
        # Try to start next item (check downstream capacity with backpressure)
        item = task.start_next(next_task, BACKPRESSURE_MULTIPLIER)
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
                output_count = 0
                pending_items = []
                
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
                if VERBOSE:
                    task_name = task.__class__.__name__.replace('Task', '')
                    with status_lock:
                        if task_name not in task_completed_items:
                            task_completed_items[task_name] = []
                        
                        # Store item with its outputs for verbose display in status
                        if pending_items:
                            task_completed_items[task_name].append((item, pending_items))
                        elif isinstance(result, list) and result:
                            task_completed_items[task_name].append((item, result))
                        elif result is not None:
                            # Single output (e.g., Write task returns output_file)
                            task_completed_items[task_name].append((item, [result]))
                        else:
                            task_completed_items[task_name].append((item, None))
                    
                    # Print status immediately after completion
                    if tasks is not None:
                        format_and_print_status(tasks, include_verbose=True)
                        # Clear completed items after printing
                        task_completed_items.clear()
                        # Reset recent counters after printing
                        for t in tasks.values():
                            t.reset_recent()
                
                # Store pending items to be added after status print
                if pending_items and hasattr(task, 'pending_queue'):
                    with status_lock:
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
            task.fail(item)
            # Don't print errors during shutdown
            if VERBOSE and not stop_event.is_set():
                print(f"Task error: {e}")


def status_printer(tasks, interval=5.0):
    """Print periodic status updates."""
    while not stop_event.is_set():
        # Acquire lock to get consistent snapshot and flush pending queues
        with status_lock:
            # Print status with verbose output
            format_and_print_status(tasks, include_verbose=True)
            
            # Clear completed items after printing
            task_completed_items.clear()
            
            # Reset recent counters after formatting
            for task in tasks.values():
                task.reset_recent()
            
            # Flush pending queue items AFTER printing status
            for name, task in tasks.items():
                if hasattr(task, 'pending_queue') and task.pending_queue:
                    for item in task.pending_queue:
                        task.add(item)
                    task.pending_queue.clear()
        
        time.sleep(interval)


def main():
    global INPUT_DIR, OUTPUT_DIR, VERBOSE
    
    # Parse arguments
    parser = argparse.ArgumentParser(description="Process images with LLM descriptions")
    parser.add_argument("input_dir", nargs="?", help="Input directory")
    parser.add_argument("output_dir", nargs="?", help="Output directory")
    parser.add_argument("--input-dir", dest="input_dir_flag", help="Input directory")
    parser.add_argument("--output-dir", dest="output_dir_flag", help="Output directory")
    parser.add_argument("--model", help="Model name")
    parser.add_argument("--prompt", help="Prompt text or @file")
    parser.add_argument("--prompt-file", help="Prompt file path")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    parser.add_argument("--sort-order", help="Sort order (natural-desc, natural-asc, name-desc, name-asc)")
    parser.add_argument("--status-interval", type=float, default=5.0, help="Status update interval in seconds")
    
    args = parser.parse_args()
    
    # Resolve paths
    INPUT_DIR = args.input_dir or args.input_dir_flag or INPUT_DIR
    if not INPUT_DIR:
        print("Error: input directory required")
        sys.exit(1)
    OUTPUT_DIR = args.output_dir or args.output_dir_flag or INPUT_DIR
    
    VERBOSE = args.verbose
    model_name = args.model or MODEL_NAME or "qwen/qwen3-vl-8b"
    sort_order = args.sort_order or SORT_ORDER
    
    # Load prompt
    prompt_text = args.prompt or os.getenv("PROMPT", "Beschrijf de foto in het Nederlands")
    if args.prompt_file:
        with open(args.prompt_file, "r", encoding="utf-8") as f:
            prompt_text = f.read().strip()
    elif prompt_text.startswith("@"):
        with open(prompt_text[1:], "r", encoding="utf-8") as f:
            prompt_text = f.read().strip()
    
    print(f"Using model: {model_name}")
    print(f"Prompt source: {'file' if args.prompt_file or (prompt_text.startswith('@')) else 'inline'}")
    print(f"Threads: skip={NUM_SKIP_CHECKER_THREADS}, download={NUM_DOWNLOAD_THREADS}, llm={NUM_LLM_THREADS}, write={NUM_WRITE_THREADS}")
    
    # Setup signal handler
    import signal as sig
    sig.signal(sig.SIGINT, signal_handler)
    
    # Load task modules
    tasks_dir = os.path.join(os.path.dirname(__file__), "tasks")
    discover_mod = load_task(os.path.join(tasks_dir, "1. discover"))
    skip_check_mod = load_task(os.path.join(tasks_dir, "2. skip_check"))
    download_mod = load_task(os.path.join(tasks_dir, "3. download"))
    llm_mod = load_task(os.path.join(tasks_dir, "4. llm"))
    write_mod = load_task(os.path.join(tasks_dir, "5. write"))
    
    # Create task instances
    discover_task = discover_mod.DiscoverTask(
        maximum=NUM_DISCOVER_THREADS,
        input_dir=INPUT_DIR,
        image_extensions=IMAGE_EXTENSIONS,
        sort_order=sort_order
    )
    
    skip_check_task = skip_check_mod.SkipCheckTask(
        maximum=NUM_SKIP_CHECKER_THREADS,
        input_dir=INPUT_DIR,
        output_dir=OUTPUT_DIR
    )
    
    download_task = download_mod.DownloadTask(
        maximum=NUM_DOWNLOAD_THREADS,
        backend_name=os.getenv("BACKEND"),
        input_dir=INPUT_DIR
    )
    
    llm_task = llm_mod.LLMTask(
        maximum=NUM_LLM_THREADS,
        model_name=model_name,
        prompt=prompt_text,
        backend_name=os.getenv("BACKEND"),
        input_dir=INPUT_DIR
    )
    
    write_task = write_mod.WriteTask(
        maximum=NUM_WRITE_THREADS,
        input_dir=INPUT_DIR,
        output_dir=OUTPUT_DIR
    )
    
    # Add root directory to discover queue
    discover_task.add(INPUT_DIR)
    
    # Build task dictionary for status
    tasks = {
        "Discover": discover_task,
        "SkipCheck": skip_check_task,
        "Download": download_task,
        "LLM": llm_task,
        "Write": write_task
    }
    
    # Start status printer
    status_thread = threading.Thread(target=status_printer, args=(tasks, args.status_interval), daemon=True)
    status_thread.start()
    
    # Start worker threads
    threads = []
    
    # Discover -> SkipCheck (discovers list of files)
    for _ in range(1):
        t = threading.Thread(
            target=worker_thread,
            args=(discover_task, skip_check_task, None, None, True, tasks),  # has_pending_queue=True, tasks
            daemon=True
        )
        t.start()
        threads.append(t)
    
    # SkipCheck -> Download (reject skipped files)
    def check_skip_rejection(result):
        should_skip, path = result
        return should_skip
    
    def skip_transform(result):
        should_skip, path = result
        return path
    
    for _ in range(NUM_SKIP_CHECKER_THREADS):
        t = threading.Thread(
            target=worker_thread,
            args=(skip_check_task, download_task, skip_transform, check_skip_rejection, False, tasks),
            daemon=True
        )
        t.start()
        threads.append(t)
    
    # Download -> LLM
    for _ in range(NUM_DOWNLOAD_THREADS):
        t = threading.Thread(
            target=worker_thread,
            args=(download_task, llm_task, None, None, False, tasks),
            daemon=True
        )
        t.start()
        threads.append(t)
    
    # LLM -> Write
    for _ in range(NUM_LLM_THREADS):
        t = threading.Thread(
            target=worker_thread,
            args=(llm_task, write_task, None, None, False, tasks),
            daemon=True
        )
        t.start()
        threads.append(t)
    
    # Write (final stage)
    for _ in range(NUM_WRITE_THREADS):
        t = threading.Thread(
            target=worker_thread,
            args=(write_task, None, None, None, False, tasks),
            daemon=True
        )
        t.start()
        threads.append(t)
    
    # Wait for completion
    try:
        while not stop_event.is_set():
            # Check if all tasks are empty (including pending_queue)
            with status_lock:
                all_empty = all(
                    len(task.queue) == 0 and 
                    len(task.active) == 0 and
                    (not hasattr(task, 'pending_queue') or len(task.pending_queue) == 0)
                    for task in tasks.values()
                )
            
            if all_empty:
                break
            
            time.sleep(0.5)
    except KeyboardInterrupt:
        stop_event.set()
    
    # Wait for threads to finish (with timeout)
    for t in threads:
        t.join(timeout=2.0)
    status_thread.join(timeout=1.0)
    
    print("\n--- DONE ---")
    
    # Print final stats
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for name, task in tasks.items():
        stats = task.stats()
        print(f"[{timestamp}] {name}: {stats['total_done']} done, {stats['total_failed']} failed")
    
    if sigint_pressed:
        print("Stopped by user (Ctrl+C)")


if __name__ == "__main__":
    main()
