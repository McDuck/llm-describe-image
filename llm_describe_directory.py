#!/usr/bin/env python3
"""
CLI wrapper for the image description pipeline.
Provides command-line interface to run image processing pipelines.
"""

import os
import sys
import argparse

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# Import configuration defaults
from config_loader import (
    DEFAULT_MODEL_NAME,
    DEFAULT_PROMPT,
    DEFAULT_OUTPUT_FORMAT,
    DEFAULT_SORT_ORDER,
    DEFAULT_NUM_SKIP_CHECKER_THREADS,
    DEFAULT_NUM_DOWNLOAD_THREADS,
    DEFAULT_NUM_LLM_THREADS,
    DEFAULT_NUM_WRITE_THREADS,
)

# Import pipeline system
from pipelines import get_pipeline, list_pipelines


def main() -> None:
    """Main entry point: parse arguments and run the specified pipeline."""
    # Get available pipelines
    available_pipelines = list_pipelines()
    
    # Parse arguments
    parser = argparse.ArgumentParser(description="Run image processing pipelines")
    parser.add_argument(
        "pipeline",
        choices=list(available_pipelines.keys()),
        help=f"Pipeline to run ({', '.join(available_pipelines.keys())})"
    )
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
    parser.add_argument("--retry", action="store_true", help="Retry all items (redo everything)")
    parser.add_argument("--retry-failed", action="store_true", help="Retry previously failed items (default: skip .error.txt files)")
    parser.add_argument("--debug", action="store_true", help="Debug mode: output raw LLM responses and input prompts")
    
    args = parser.parse_args()
    
    # Get the pipeline
    pipeline = get_pipeline(args.pipeline)
    if not pipeline:
        print(f"Error: pipeline '{args.pipeline}' not found")
        sys.exit(1)
    
    # Resolve input directory
    input_dir = args.input_dir or args.input_dir_flag or os.getenv("INPUT_DIR")
    if not input_dir:
        print("Error: input directory required")
        sys.exit(1)
    
    # Resolve output directory (defaults to input_dir)
    output_dir = args.output_dir or args.output_dir_flag or os.getenv("OUTPUT_DIR") or input_dir
    
    # Resolve model name
    model_name = args.model or os.getenv("MODEL_NAME") or DEFAULT_MODEL_NAME
    
    # Resolve sort order
    sort_order = args.sort_order or os.getenv("SORT_ORDER", DEFAULT_SORT_ORDER)
    
    # Load prompt
    prompt_text = args.prompt or os.getenv("PROMPT", DEFAULT_PROMPT)
    if args.prompt_file:
        with open(args.prompt_file, "r", encoding="utf-8") as f:
            prompt_text = f.read().strip()
    elif prompt_text.startswith("@"):
        with open(prompt_text[1:], "r", encoding="utf-8") as f:
            prompt_text = f.read().strip()
    
    # Print configuration
    print(f"Using model: {model_name}")
    print(f"Prompt source: {'file' if args.prompt_file or (prompt_text.startswith('@')) else 'inline'}")
    print(f"Threads: skip={DEFAULT_NUM_SKIP_CHECKER_THREADS}, download={DEFAULT_NUM_DOWNLOAD_THREADS}, llm={DEFAULT_NUM_LLM_THREADS}, write={DEFAULT_NUM_WRITE_THREADS}")
    
    # Set environment variables for pipeline to use
    if args.model:
        os.environ["MODEL_NAME"] = model_name
    if args.sort_order:
        os.environ["SORT_ORDER"] = sort_order
    if args.prompt:
        os.environ["PROMPT"] = prompt_text
    if args.retry_failed:
        os.environ["RETRY_FAILED"] = "true"
    
    # Set debug flag if applicable
    if args.debug and hasattr(pipeline, 'debug'):
        pipeline.debug = True
    
    # Set retry flags if applicable
    if args.retry and hasattr(pipeline, 'skip_all'):
        pipeline.skip_all = True
    elif args.retry_failed and hasattr(pipeline, 'retry_failed'):
        pipeline.retry_failed = True
    
    # Run the pipeline
    pipeline.run(
        input_dir=input_dir,
        output_dir=output_dir,
        verbose=args.verbose,
        status_interval=args.status_interval
    )


if __name__ == "__main__":
    main()
