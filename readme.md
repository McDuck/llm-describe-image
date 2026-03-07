# llm‑describe‑image (v1.0.1)

Task‑based image description pipelines powered by vision‑capable large language models (LLMs).

This repository provides a command‑line utility and associated Python library for
automatically generating and enhancing textual descriptions of images stored in a
directory.  The core is a flexible, multithreaded pipeline system in which each
stage is implemented as a self‑contained "task".  A default pipeline discovers
image files, downloads them if necessary, invokes an LLM to caption each image,
and writes the results to `.txt` files.  Additional pipelines (for example,
context‑aware enhancement) can be added by subclassing the `Pipeline` base class.

---

## 🔍 Features

- Describe images using any vision‑capable model supported by the `lmstudio` SDK
  (e.g. GPT‑4o Vision).
- Modular, thread‑aware pipeline architecture:
  - discovery, skip checking, downloading
  - LLM inference, writing results
  - optional context gathering and enhancement
- Configurable via CLI flags, environment variables, or `config.defaults.toml`.
- Built‑in retry and error‑handling logic.
- Extensible: add new pipelines or tasks without rewriting the core engine.
- Lightweight dependencies; works in a standard Python 3.8+ virtual environment.

## 🚀 Installation

```bash
# clone repository
git clone https://github.com/McDuck/llm-describe-image.git
cd llm-describe-image

# (optional but recommended) create a virtual environment
python -m venv .venv
# activate it on Windows:
# & .venv\Scripts\Activate.ps1
# or on Unix:
# source .venv/bin/activate

# install package in editable mode
pip install -e .
```

A `llm-describe-image` script will be installed, and you can also invoke the CLI
with `python -m llm_describe_directory`.

## 🛠 Quick Start

Generate descriptions for all images under `~/photos`:

```bash
llm-describe-image describe-image ~/photos
```

Each image will have a corresponding `<image>.txt` file containing the LLM's
caption.

Enhance existing descriptions by leveraging surrounding images:

```bash
llm-describe-image enhance-by-context ~/photos
```

### Example with options

```bash
llm-describe-image describe-image \
    ~/photos/vacation2024 \
    --model gpt-4o-vision-preview \
    --prompt "Write a concise caption for this photo" \
    --verbose
```

## ⚙️ Configuration

Options may be supplied on the command line or via environment variables.  Run
`llm_describe_directory.py --help` for a complete list.  Common environment
variables include:

| Variable               | Default (from `config.defaults.toml`) | Description |
|------------------------|---------------------------------------|-------------|
| `MODEL_NAME`           | `describe.model`                     | Model to use for inference |
| `PROMPT`               | `describe.prompt`                    | Prompt text or `@/path/to/file` |
| `OUTPUT_FORMAT`        | `describe.output_format`             | Output filename template |
| `SORT_ORDER`           | `sorting.order` (`natural-desc`)      | File sort order |
| `PIPELINE`             | `describe-image`                     | Pipeline to run |
| `CONTEXT_MODEL`        | same as `MODEL_NAME`                 | Model for context pipeline |
| `CONTEXT_PROMPT`        | `enhancement.prompt`                 | Prompt used during enhancement |
| `CONTEXT_WINDOW_DAYS`  | `10`                                 | Days before/after to consider |
| `MAX_CONTEXT_ITEMS`    | `20`                                 | Max number of context images |
| `RETRY_FAILED`         | `false`                              | Retry only previously failed items |

Thread counts, backpressure multiplier, image extensions and other settings
can also be adjusted in the TOML file or via environment variables.  The file
`config.defaults.toml` documents all available defaults.

## 🔁 Pipelines

Two pipelines are included by default; additional pipelines can be added by the
user.

### 📄 describe-image (default)

```
Discover → SkipCheck → Download → LLM → Write
```

- Discovers image files under the input directory.
- Skips files that already have a description (`.txt`).
- Downloads remote images if needed.
- Sends each image to the configured LLM along with optional metadata.
- Writes the resulting text to `<image>.txt`.

### 🧠 enhance-by-context

```
Discover → SkipCheck → Context → Enhance → WriteEnhanced
```

This pipeline reads existing `<image>.txt` descriptions and attempts to improve
them by gathering context from temporally and spatially adjacent images.  It
uses a separate prompt and (optionally) a different model.  See
[`CONTEXT_PIPELINE.md`](CONTEXT_PIPELINE.md) for detailed behavior and tuning
tips.

### ✏️ Creating your own pipeline

To extend the system with a new pipeline:

1. Create a new subdirectory under `pipelines/` (e.g. `pipelines/my_new/`) with
   a `task.py` that defines a subclass of `Pipeline` and implements `run()`.
2. Register the pipeline in `pipelines/__init__.py` (add it to
   `get_pipeline()` and `list_pipelines()`).
3. Add any new task modules under `tasks/` if you require custom steps.

The `Pipeline` base class handles worker thread orchestration, status updates,
backpressure, and graceful shutdown; your subclass only needs to declare its
configuration and call `add_task()`.

## 🧩 Architecture Overview

- **`llm_describe_directory.py`** – CLI wrapper that selects and executes a
  pipeline.
- **`pipelines/`** – Contains pipeline definitions and the base class.
- **`tasks/`** – Individual task implementations (discover, skip_check,
  download, llm, write, context, etc.).
- **`config_loader.py`** – Loads default configuration from `config.defaults.toml`.
- **`llms/`** – Abstraction layer over various LLM backends (lmstudio etc.).

The pipeline engine spins up a configurable number of worker threads for each
task and communicates via in‑memory queues.  Status lines are printed at regular
intervals or immediately after each item if `--verbose` is enabled.

## 🛠 Development

Install development dependencies:

```bash
pip install -e .[dev]
```

Run the formatter and type checker:

```bash
black .
mypy .
```

Tests are currently sparse; the `tests/` directory serves as a placeholder.
Contributions are welcome!

## 📄 License

MIT © McDuck

