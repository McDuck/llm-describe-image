import lmstudio as lms
import os
import threading
import queue
import signal
import sys
import time
import subprocess
import shutil
import argparse
import re
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    # If python-dotenv is not installed, we simply won't load a .env file.
    pass

# --- CONFIG ---
# Load defaults from .env if present
INPUT_DIR = os.getenv("INPUT_DIR")
OUTPUT_DIR = os.getenv("OUTPUT_DIR")
NUM_DOWNLOAD_THREADS = int(os.getenv("NUM_DOWNLOAD_THREADS", "2"))
NUM_LLM_THREADS = int(os.getenv("NUM_LLM_THREADS", "1"))
MAX_DOWNLOADED_BEFORE_LLM = int(os.getenv("MAX_DOWNLOADED_BEFORE_LLM", "8"))  # Maximum number of downloaded images waiting for LLM processing
MAX_DISCOVERY = int(os.getenv("MAX_DISCOVERY", "10000"))  # Safety limit: maximum files to discover in one run
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp"}
MODEL_NAME = os.getenv("MODEL_NAME")
MODEL_MIN_VRAM_GB = int(os.getenv("MODEL_MIN_VRAM_GB", "12"))  # Minimum free GPU memory required to attempt loading this model (best-effort check)

# --- GLOBALS ---
stop_event = threading.Event()
discovered_count = 0
to_download_count = 0
downloading_count = 0
queued_count = 0
llm_count = 0
llmed_count = 0
skipped_count = 0
failed_count = 0
processed_count = 0
# Directory traversal stats (best-effort, no pre-count to avoid blocking)
dirs_scanned = 0
last_action_lock = threading.Lock()
last_action = "initializing"
sigint_pressed = False
VERBOSE = False
server_started_by_script = False
model_loaded_by_script = False
model_name_loaded_by_script = None
model_was_preloaded = False
last_file_processed = None

download_queue = queue.Queue()
llm_queue = queue.Queue()
llm_queue_semaphore = None  # Will be initialized in main() with MAX_DOWNLOADED_BEFORE_LLM
slots_in_use = 0  # Tracks semaphore-acquired slots (downloaded images reserved for LLM)


# --- CTRL+C HANDLER ---
def signal_handler(sig, frame):
    # Do not print or call sys.exit here to avoid reentrant stdout issues while
    # threads are running; just set the stop event and let the main thread
    # raise KeyboardInterrupt or exit naturally.
    global sigint_pressed
    sigint_pressed = True
    stop_event.set()


signal.signal(signal.SIGINT, signal_handler)


# --- UTILS ---
def safe_mkdir(path):
    if not os.path.exists(path):
        os.makedirs(path)


def get_output_path(input_path):
    relative = os.path.relpath(input_path, INPUT_DIR)
    output_file = os.path.join(OUTPUT_DIR, relative + ".txt")
    safe_mkdir(os.path.dirname(output_file))
    return output_file


def print_status(message="", force=False):
    # Only print per-action status messages when verbose is enabled, unless
    # `force` is set (used by the periodic status printer).
    if not force and not globals().get("VERBOSE", False):
        return
    # Calculate unprocessed: items discovered but not yet fully processed
    # Includes: to_download waiting in download queue, queued waiting for LLM, and currently LLM-ing
    unprocessed = to_download_count + queued_count + llm_count
    print(
        f"{message} Discovered: {discovered_count}, ToDownload: {to_download_count}, "
        f"Downloading: {downloading_count}, Queued: {queued_count}, "
        f"LLM: {llm_count}, LLMed: {llmed_count}, Unprocessed: {unprocessed}, Skipped: {skipped_count}, "
        f"Failed: {failed_count}, Processed: {processed_count}"
    )


def print_verbose(message):
    """Only print if VERBOSE is enabled."""
    if globals().get("VERBOSE", False):
        print(message)


def cleanup_server_and_model():
    """Stop server and unload model if the script started/loaded them."""
    global server_started_by_script, model_loaded_by_script, model_was_preloaded, model_name_loaded_by_script
    cli = shutil.which("lms")
    if not cli:
        return
    
    # Only unload if we loaded the model AND it wasn't already loaded when we started
    should_unload = model_loaded_by_script and not model_was_preloaded
    
    if should_unload:
        # Check if a model is actually loaded before trying to unload
        try:
            set_last_action("checking if model needs unloading")
            result = subprocess.run([cli, "ps"], capture_output=True, text=True, timeout=5)
            if result.returncode == 0 and result.stdout.strip():
                # Only unload if there's an active model session
                lines = result.stdout.strip().split('\n')
                if len(lines) > 1:  # More than just the header
                    set_last_action("unloading model")
                    print_verbose(f"Unloading model {model_name_loaded_by_script}...")
                    subprocess.run([cli, "unload", model_name_loaded_by_script], check=False, timeout=10)
                    print_verbose("Model unloaded")
        except Exception as e:
            print_verbose(f"Failed to unload model: {e}")
    
    if server_started_by_script:
        try:
            set_last_action("stopping server")
            print_verbose("Stopping LM Studio server...")
            subprocess.run([cli, "server", "stop"], check=False, timeout=5)
            print_verbose("Server stopped")
        except Exception as e:
            print_verbose(f"Failed to stop server: {e}")


def set_last_action(action: str):
    global last_action
    with last_action_lock:
        last_action = action


def get_last_action():
    with last_action_lock:
        return last_action


# --- WORKERS ---
def download_worker():
    global downloading_count, to_download_count, queued_count, failed_count, last_file_processed, slots_in_use
    while not stop_event.is_set():
        try:
            # Smarter waiting message: if download queue is empty but LLM queue has items,
            # we're really waiting for LLM to finish, not for images to download
            if download_queue.empty() and not llm_queue.empty():
                if last_file_processed:
                    set_last_action(f"waiting for LLM to finish (last: {last_file_processed})")
                else:
                    set_last_action("waiting for LLM to finish")
            elif last_file_processed:
                set_last_action(f"waiting for images to download (last: {last_file_processed})")
            else:
                set_last_action("waiting for images to download")
            input_path = download_queue.get(timeout=0.5)
        except queue.Empty:
            continue

        # Acquire semaphore slot before downloading (blocks if queue is full)
        # This strictly enforces the MAX_DOWNLOADED_BEFORE_LLM limit
        acquired = False
        while not stop_event.is_set():
            acquired = llm_queue_semaphore.acquire(timeout=0.5)
            if acquired:
                slots_in_use += 1
                break
            # Show waiting status while blocked on semaphore
            if last_file_processed:
                set_last_action(f"waiting for LLM (queue full: {slots_in_use}/{MAX_DOWNLOADED_BEFORE_LLM}, last: {last_file_processed})")
            else:
                set_last_action(f"waiting for LLM (queue full: {slots_in_use}/{MAX_DOWNLOADED_BEFORE_LLM})")
        
        if stop_event.is_set():
            if acquired:
                slots_in_use = max(0, slots_in_use - 1)
                llm_queue_semaphore.release()
            download_queue.task_done()
            continue

        downloading_count += 1
        to_download_count -= 1
        rel = os.path.relpath(input_path, INPUT_DIR)
        last_file_processed = rel
        set_last_action(f"downloading file {rel}")
        print_status(f"Downloading {rel}.")

        try:
            image_handle = lms.prepare_image(input_path)
            llm_queue.put((input_path, image_handle))
            queued_count += 1
        except Exception as e:
            print(f"Error preparing {input_path}: {e}")
            failed_count += 1
            # Release semaphore on error since we won't be adding to LLM queue
            slots_in_use = max(0, slots_in_use - 1)
            llm_queue_semaphore.release()

        downloading_count -= 1
        download_queue.task_done()
        set_last_action(f"queued file {rel}")
        print_status(f"Queued {rel}.")


def llm_worker(model, prompt):
    global llm_count, llmed_count, queued_count, processed_count, failed_count, last_file_processed, slots_in_use
    while not stop_event.is_set():
        try:
            # Show what we're waiting for based on queue state
            if llm_queue.empty() and not download_queue.empty():
                if last_file_processed:
                    set_last_action(f"waiting for downloads to complete (last: {last_file_processed})")
                else:
                    set_last_action("waiting for downloads to complete")
            elif last_file_processed:
                set_last_action(f"waiting for images to process (last: {last_file_processed})")
            else:
                set_last_action("waiting for images to process")
            input_path, image_handle = llm_queue.get(timeout=0.5)
        except queue.Empty:
            continue

        llm_count += 1
        queued_count -= 1
        rel = os.path.relpath(input_path, INPUT_DIR)
        last_file_processed = rel
        set_last_action(f"processing file {rel}")
        print_status(f"LLM {rel}.")

        output_file = get_output_path(input_path)
        try:
            chat = lms.Chat()
            chat.add_user_message(prompt, images=[image_handle])
            result = model.respond(chat)
            # Prefer result.content if available (matches the simpler script),
            # otherwise fallback to stringifying the result.
            content = getattr(result, "content", None)
            if content is None:
                content = str(result)
            with open(output_file, "w", encoding="utf-8") as f:
                f.write(content)
            llmed_count += 1
            processed_count += 1
            out_rel = os.path.relpath(output_file, OUTPUT_DIR)
            print_status(f"LLMed {rel}. Wrote {out_rel}")
        except Exception as e:
            print(f"Failed LLM {input_path}: {e}")
            failed_count += 1

        llm_count -= 1
        llm_queue.task_done()
        # Release semaphore slot to allow another download
        slots_in_use = max(0, slots_in_use - 1)
        llm_queue_semaphore.release()


# --- MAIN ---
def main():
    global discovered_count, to_download_count, skipped_count, INPUT_DIR, OUTPUT_DIR, llm_queue_semaphore, dirs_scanned

    # Parse CLI options
    parser = argparse.ArgumentParser()
    # Positional args for compatibility with the non-opt script; optional so we
    # can still support the '--input-dir'/'--output-dir' flags and .env defaults.
    parser.add_argument("input_dir", nargs="?", default=None, help="(positional) Path to input directory")
    parser.add_argument("output_dir", nargs="?", default=None, help="(positional) Path to output directory")
    parser.add_argument("--input-dir", dest="input_dir_flag", default=None, help="Path to input directory containing images (required if not set in .env)")
    parser.add_argument("--output-dir", dest="output_dir_flag", default=None, help="Path to output directory for text files; defaults to input directory if not supplied")
    # Default to env var value, or fall back to the simple script default
    default_model = MODEL_NAME if MODEL_NAME is not None else "qwen/qwen3-vl-8b"
    parser.add_argument("--model", default=default_model, help="Vision-enabled model to use")
    parser.add_argument("--prompt", default="Beschrijf de foto in het Nederlands", help="Prompt to describe the image")
    parser.add_argument("--auto-start", action="store_true", help="Automatically start LM Studio server via CLI without prompting")
    parser.add_argument("--no-install-model", action="store_true", help="Do not automatically install/load model via the LM Studio CLI if missing")
    parser.add_argument("--status-interval", dest="status_interval", type=float, default=None, help="How often (seconds) to print periodic status. Overrides STATUS_INTERVAL env var.")
    parser.add_argument("-v", "--verbose", dest="verbose", action="store_true", help="Enable verbose logging of per-file actions (Downloading/Queued/LLM/Skipped)")
    # Keep CLI minimal to mirror the simpler script
    args = parser.parse_args()
    # Allow overriding paths from CLI
    # Determine final input/output directory precedence: positional > flag > .env
    INPUT_DIR = args.input_dir or args.input_dir_flag or INPUT_DIR
    if not INPUT_DIR:
        print("Error: input directory is required. Provide --input-dir or set INPUT_DIR in .env")
        sys.exit(1)
    OUTPUT_DIR = args.output_dir or args.output_dir_flag or INPUT_DIR

    # No special bootstrapping behavior in the opt script; mimic the simple script.

    # Avoid explicitly bootstrapping the LM Studio API; instead, follow the
    # behavior of `llm-describe-directory.py`: try to instantiate the model via
    # the SDK directly and fail fast if the SDK can't instantiate it.

    # Attempt to instantiate the requested model like the simple script does.
    model = None
    # Determine the model name to try: CLI arg > env var > built-in default
    model_name = args.model or MODEL_NAME or "qwen/qwen3-vl-8b"
    # Verbose flag controls most prints of per-file actions.
    global VERBOSE
    VERBOSE = bool(args.verbose)
    if VERBOSE:
        print(f"Using model: {model_name}")
    
    # Initialize semaphore to strictly enforce queue limit
    llm_queue_semaphore = threading.Semaphore(MAX_DOWNLOADED_BEFORE_LLM)
    
    # Start periodic status thread early so it shows progress during server/model setup
    status_interval = float(os.getenv("STATUS_INTERVAL", "5.0"))
    if args.status_interval is not None:
        status_interval = max(0.1, float(args.status_interval))
    def status_printer(interval: float):
        while not stop_event.is_set():
            la = get_last_action()
            print_status(f"Status - {la} | Dirs: {dirs_scanned}", force=True)
            time.sleep(interval)

    status_thread = threading.Thread(target=status_printer, args=(status_interval,), daemon=True)
    status_thread.start()
    
    def free_gpu_memory_gb():
        # Try using nvidia-smi if available; return free GPU memory in GB (largest GPU), or None
        nvidia = shutil.which("nvidia-smi")
        if not nvidia:
            return None
        try:
            out = subprocess.check_output([nvidia, "--query-gpu=memory.free", "--format=csv,noheader,nounits"], encoding="utf-8")
            vals = [int(x.strip()) for x in out.splitlines() if x.strip().isdigit()]
            if not vals:
                return None
            # memory.free is in MiB
            return max(vals) / 1024.0
        except Exception:
            return None

    def cli_load_model_if_needed(model_name: str):
        # If the model doesn't appear to be loadable via the SDK, try the CLI
        # `lms model load <model>` if it's available on PATH.
        cli = shutil.which("lms")
        if not model_name:
            print_verbose("Refusing to load model via CLI: model name is empty or None.")
            return False
        if not cli:
            return False
        try:
            set_last_action(f"loading model {model_name} via CLI")
            print_verbose(f"Attempting to load model {model_name} via CLI (lms load)")
            # Start the load process
            proc = subprocess.Popen([cli, "load", model_name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            # Wait for it to complete, updating status periodically
            while proc.poll() is None:
                set_last_action(f"waiting for model {model_name} to load")
                time.sleep(2)
            return proc.returncode == 0
        except Exception as e:
            print(f"Failed to load model via CLI: {e}")
            return False

    def start_server_via_cli():
        # Use the CLI if available to start the server; return True if started successfully
        cli = shutil.which("lms")
        if cli:
            set_last_action("starting server via CLI")
            print_verbose("Starting LM Studio server via CLI... (lms server start)")
            try:
                subprocess.Popen([cli, "server", "start"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                return True
            except Exception as e:
                print(f"Failed to start LM Studio server with CLI: {e}")
                return False
        return False

    def check_lms_server_running():
        """Try multiple ways to determine if LM Studio server is running.
        1) Try SDK bootstrap() first.
        2) If SDK fails, try the `lms` CLI commands to detect server/model status.
        Returns (running: bool, detected_via: str).
        """
        # Try SDK bootstrap
        try:
            lms.bootstrap()
            set_last_action("sdk bootstrap success")
            return True, "sdk"
        except Exception:
            pass

        # Try the CLI as a fallback
        cli = shutil.which("lms")
        if not cli:
            return False, "none"

        cmds = [
            [cli, "server", "status"],
            [cli, "status"],
            [cli, "server", "list"],
            [cli, "model", "list"],
        ]
        for cmd in cmds:
            try:
                set_last_action(f"checking server via CLI: {' '.join(cmd)}")
                out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, timeout=2, encoding="utf-8")
                text = out.strip().lower()
                if not text:
                    continue
                # Look for 'server: on' or 'loaded models' or anything that suggests server is active
                if "server: on" in text or "on (port" in text or "loaded models" in text or "model" in text:
                    return True, "cli"
                # If the CLI returned any meaningful text, assume server is responding
                return True, "cli"
            except Exception:
                continue

        return False, "none"

    # Check if server is running and optionally prompt to start it.
    server_running, detected_via = check_lms_server_running()
    if server_running:
        print_verbose(f"LM Studio server detected (via {detected_via})")

    if not server_running:
        global server_started_by_script
        if args.auto_start:
            set_last_action("server not running; starting via CLI")
            print_verbose("LM Studio server does not appear to be running; attempting to start via CLI...")
            if start_server_via_cli():
                server_started_by_script = True
                set_last_action("waiting for server to start")
                time.sleep(2)
                try:
                    lms.bootstrap()
                    server_running = True
                except Exception:
                    server_running = False
        else:
            print_verbose("LM Studio server does not appear to be running.")
            yn = input("Start it now? [y/N]: ")
            if yn.strip().lower() in ("y", "yes"):
                if start_server_via_cli():
                    server_started_by_script = True
                    set_last_action("waiting for server to start")
                    time.sleep(2)
                    try:
                        lms.bootstrap()
                        server_running = True
                    except Exception:
                        server_running = False

    # Attempt to instantiate the model like the non-opt version.
    # If no model name is provided at all, fail early with a clear message.
    if not model_name:
        print("Error: No model specified. Provide --model, set MODEL_NAME in .env, or use the built-in default.")
        sys.exit(1)

    # Check if model is already loaded before we try to load it
    global model_was_preloaded, model_name_loaded_by_script, model_loaded_by_script
    cli = shutil.which("lms")
    model_was_loaded_before = False
    if cli:
        try:
            result = subprocess.run([cli, "ps"], capture_output=True, text=True, timeout=2)
            if result.returncode == 0 and result.stdout.strip():
                lines = result.stdout.strip().split('\n')
                if len(lines) > 1:  # More than just the header
                    model_was_loaded_before = True
                    model_was_preloaded = True
                    print_verbose(f"Model already loaded before script started")
        except Exception:
            pass

    set_last_action(f"loading model {model_name}")
    try:
        model = lms.llm(model_name)
        # If model instantiation succeeded but wasn't loaded before, we must have loaded it
        if not model_was_loaded_before:
            model_loaded_by_script = True
            model_name_loaded_by_script = model_name
            print_verbose(f"Model {model_name} loaded by SDK")
    except Exception as e:
        print(f"Failed to instantiate model {model_name} via SDK: {e}")
        # If automatic install is disabled, don't attempt CLI load.
        if args.no_install_model or not model_name:
            print("Model not installed and automatic CLI install is disabled via --no-install-model. Exiting.")
            sys.exit(1)
        # Prompt before installing via CLI
        yn = input(f"Model {model_name} not installed. Attempt to load it via 'lms model load' now? [y/N]: ")
        if yn.strip().lower() in ("y", "yes"):
            if cli_load_model_if_needed(model_name):
                model_loaded_by_script = True
                model_name_loaded_by_script = model_name
                try:
                    model = lms.llm(model_name)
                except Exception as e2:
                    print(f"Retry after CLI load still failed: {e2}")
        # If still no model, exit (like the simple script).
        if model is None:
            print("Failed to instantiate model and CLI fallback failed. Exiting.")
            cleanup_server_and_model()
            sys.exit(1)

    # Start download and LLM worker threads
    for _ in range(NUM_DOWNLOAD_THREADS):
        t = threading.Thread(target=download_worker, daemon=True)
        t.start()
    # Only start LLM worker threads if we successfully have a model loaded.
    if model is not None and NUM_LLM_THREADS > 0:
        for _ in range(NUM_LLM_THREADS):
            t = threading.Thread(target=llm_worker, args=(model, args.prompt), daemon=True)
            t.start()
    else:
        print("LLM model not available; skipping LLM worker startup.")

    try:
        # --- DISCOVERY ---
        # Start discovery immediately; do not pre-count directories to avoid blocking
        stop_discovery = False
        printed_limit_warning = False
        for root, dirs, files in os.walk(INPUT_DIR):
            # Track how many directories have been scanned
            dirs_scanned += 1
            # Natural descending sort of directories and files
            def _natural_key(s: str):
                return [int(t) if t.isdigit() else t.lower() for t in re.split(r'(\d+)', s)]
            dirs[:] = sorted(dirs, key=_natural_key, reverse=True)
            files = sorted(files, key=_natural_key, reverse=True)
            for file in files:
                if stop_event.is_set():
                    break
                rel_root = os.path.relpath(root, INPUT_DIR)
                set_last_action(f"scanning {rel_root}")
                ext = os.path.splitext(file)[1].lower()
                if ext not in IMAGE_EXTENSIONS:
                    continue

                input_path = os.path.join(root, file)
                output_file = get_output_path(input_path)
                discovered_count += 1

                if os.path.exists(output_file):
                    skipped_count += 1
                    rel = os.path.relpath(input_path, INPUT_DIR)
                    print_status(f"Skipped {rel}.")
                    continue

                # Safety limit to prevent runaway discovery
                if to_download_count >= MAX_DISCOVERY:
                    if not printed_limit_warning:
                        print(f"\nWarning: Hit discovery limit of {MAX_DISCOVERY} files. Stopping discovery.")
                        print(f"Increase MAX_DISCOVERY in .env if you need to process more files.")
                        printed_limit_warning = True
                    stop_discovery = True
                    break

                to_download_count += 1
                download_queue.put(input_path)
                rel = os.path.relpath(input_path, INPUT_DIR)
                set_last_action(f"discovered file {rel}")
                print_status(f"Discovered {rel}.")

            if stop_discovery:
                break

        # Wait for queues to finish
        while (not download_queue.empty() or not llm_queue.empty() or
               downloading_count > 0 or llm_count > 0):
            if stop_event.is_set():
                break
            time.sleep(0.5)
    finally:
        # Always cleanup server/model if we started them, even on Ctrl+C
        cleanup_server_and_model()

    print("\n--- DONE ---")
    # Always print the final status summary regardless of verbose mode.
    print_status(force=True)
    if sigint_pressed:
        print(f"Stopped by user (Ctrl+C). Last action: {get_last_action()}")
    else:
        print(f"Final last_action: {get_last_action()}")


if __name__ == "__main__":
    main()
