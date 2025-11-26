import argparse
import lmstudio as lms
import sys
import os
from pathlib import Path

# ----------------------------
# Argument parsing
# ----------------------------
parser = argparse.ArgumentParser(description="Recursively describe images with LM Studio VLM")
parser.add_argument("input_dir", type=str, help="Path to input directory containing images")
parser.add_argument("output_dir", type=str, help="Path to output directory for text files")
parser.add_argument("--prompt", type=str, default="Beschrijf wat je ziet op de bijgevoegde afbeelding.", help="Prompt to describe the image")
parser.add_argument("--model", type=str, default="qwen/qwen3-vl-8b", help="Vision-enabled model to use")
args = parser.parse_args()

input_dir = Path(args.input_dir)
output_dir = Path(args.output_dir)

if not input_dir.is_dir():
    print(f"Error: Input directory not found: {input_dir}")
    sys.exit(1)

# ----------------------------
# Load model
# ----------------------------
try:
    model = lms.llm(args.model)
except Exception as e:
    print(f"Error loading model {args.model}: {e}")
    sys.exit(1)

# ----------------------------
# Helper function to check if file is image
# ----------------------------
def is_image(file_path):
    ext = file_path.suffix.lower()
    return ext in [".jpg", ".jpeg", ".png", ".webp"]

# ----------------------------
# Recursive processing
# ----------------------------
for root, dirs, files in os.walk(input_dir):
    for file in files:
        file_path = Path(root) / file
        if not is_image(file_path):
            continue  # skip non-images (videos etc.)

        # Compute relative path from input_dir
        rel_path = file_path.relative_to(input_dir)
        output_file = output_dir / rel_path.with_suffix(rel_path.suffix + ".txt")

        # Skip if output file already exists
        if output_file.exists():
            print(f"Skipping existing file: {output_file}")
            continue

        # Ensure parent directories exist
        output_file.parent.mkdir(parents=True, exist_ok=True)

        print(f"Processing {file_path} → {output_file}")

        try:
            image_handle = lms.prepare_image(str(file_path))
            chat = lms.Chat()
            chat.add_user_message(args.prompt, images=[image_handle])
            prediction = model.respond(chat)

            # Write result to file
            with open(output_file, "w", encoding="utf-8") as f:
                f.write(prediction.content)

        except Exception as e:
            print(f"Error processing {file_path}: {e}")
