#!/usr/bin/env python3
"""
CLI wrapper for the image description pipeline.
Provides command-line interface to run image processing pipelines.
"""

import os
import sys
import argparse
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent / ".env")
except Exception:
    pass

# Import configuration defaults
from describe_media.config_loader import (
    DEFAULT_MODEL_NAME,
    DEFAULT_PROMPT,
    DEFAULT_OUTPUT_FORMAT,
    DEFAULT_SORT_ORDER,
    DEFAULT_NUM_SKIP_CHECKER_THREADS,
    DEFAULT_NUM_DISCOVER_THREADS,
    DEFAULT_NUM_DOWNLOAD_THREADS,
    DEFAULT_NUM_ENHANCE_THREADS,
    DEFAULT_NUM_RESIZE_THREADS,
    DEFAULT_RECOGNITION_THREADS,
    DEFAULT_NUM_LLM_THREADS,
    DEFAULT_NUM_WRITE_THREADS,
    DEFAULT_VIDEO_FRAME_INTERVAL_SECONDS,
    DEFAULT_VIDEO_MAX_FRAMES,
)

# Import pipeline system
from describe_media.pipelines import get_pipeline, list_pipelines


def main() -> None:
    """Run the default combined workflow or one explicitly named pipeline."""
    # Get available pipelines
    available_pipelines = list_pipelines()
    
    # Parse arguments
    parser = argparse.ArgumentParser(description="Run media processing pipelines")
    parser.add_argument(
        "pipeline_or_input",
        nargs="?",
        help=(
            "Optional pipeline name. If omitted, the describe pipeline runs; "
            "a non-pipeline value is treated as INPUT_DIR."
        ),
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
    parser.add_argument(
        "--subdirectory",
        help="Limit recognition-cluster preparation to a directory relative to INPUT_DIR while preserving source-relative output paths",
    )
    parser.add_argument(
        "--manifest",
        help="Recognition-only text file containing one source-relative image path per line",
    )
    parser.add_argument(
        "--random-sample",
        type=int,
        metavar="COUNT",
        help="Recognition-only: prepare COUNT distinct random, not-yet-reviewed source images",
    )
    parser.add_argument("--retry", action="store_true", help="Retry all items (redo everything)")
    parser.add_argument("--retry-failed", action="store_true", help="Retry previously failed items (default: skip .error.txt files)")
    parser.add_argument("--debug", action="store_true", help="Debug mode: output raw LLM responses and input prompts")
    
    args = parser.parse_args()

    # A bare command runs the integrated describe graph.  Geolocation and the
    # recognition workflows remain explicit maintenance commands.
    if args.pipeline_or_input in available_pipelines:
        pipeline_name = args.pipeline_or_input
    else:
        pipeline_name = "describe"
        if args.pipeline_or_input:
            args.output_dir = args.input_dir or args.output_dir
            args.input_dir = args.pipeline_or_input
    
    # Get the pipeline
    pipeline = get_pipeline(pipeline_name)
    if not pipeline:
        print(f"Error: pipeline '{pipeline_name}' not found")
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
    
    # Print only configuration relevant to the selected workflow. Recognition
    # training does not invoke the LLM or use its prompt/thread settings.
    if pipeline.name == "describe":
        context_size_info = ""
        if hasattr(pipeline, 'model_context_length'):
            context_size_info = f" with context size {pipeline.model_context_length}"
        print(f"Using model: {model_name}{context_size_info}")
        print(f"Prompt source: {'file' if args.prompt_file or (prompt_text.startswith('@')) else 'inline'}")
        print("Describe graph: Discover[d>div] -> images|frames -> ImageRouter -> Resize/Metadata/Geolocate -> Recognition -> LLM -> Enhance; source images also -> Shortcut; JPEG LLM errors -> FixJPEG")
        print(
            "Threads: "
            f"discover={DEFAULT_NUM_DISCOVER_THREADS}, "
            f"extract={DEFAULT_NUM_DOWNLOAD_THREADS}, "
            f"router={DEFAULT_NUM_DOWNLOAD_THREADS}, "
            f"metadata={DEFAULT_NUM_DOWNLOAD_THREADS}, "
            f"resize={DEFAULT_NUM_RESIZE_THREADS}, "
            f"recognition={DEFAULT_RECOGNITION_THREADS}, "
            f"geolocate=1, llm={DEFAULT_NUM_LLM_THREADS}, "
            f"enhance={DEFAULT_NUM_ENHANCE_THREADS}, shortcut=10, fix-jpeg=1"
        )
        print(
            "Video sampling: "
            f"every {os.getenv('VIDEO_FRAME_INTERVAL_SECONDS', DEFAULT_VIDEO_FRAME_INTERVAL_SECONDS)}s, "
            f"maximum {os.getenv('VIDEO_MAX_FRAMES', DEFAULT_VIDEO_MAX_FRAMES)} frames"
        )
    elif pipeline.name != "recognition-train":
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
    if args.retry and hasattr(pipeline, 'retry'):
        pipeline.retry = True
    elif args.retry_failed and hasattr(pipeline, 'retry_failed'):
        pipeline.retry_failed = True
    
    # Run the pipeline
    pipeline.run(
        input_dir=input_dir,
        output_dir=output_dir,
        verbose=args.verbose,
        status_interval=args.status_interval,
        subdirectory=args.subdirectory,
        manifest_path=args.manifest,
        random_sample_size=args.random_sample,
    )


if __name__ == "__main__":
    main()
