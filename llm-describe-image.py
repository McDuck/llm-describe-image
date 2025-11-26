import argparse
import lmstudio as lms
import sys
import os

# ----------------------------
# Argument parsing
# ----------------------------
parser = argparse.ArgumentParser(description="Describe an image with LM Studio VLM")
parser.add_argument("image", type=str, help="Path to the image file")
parser.add_argument("--prompt", type=str, default="Beschrijf wat je ziet op de bijgevoegde afbeelding.", help="Prompt to describe the image")
parser.add_argument("--model", type=str, default="qwen/qwen3-vl-8b", help="Vision-enabled model to use")
args = parser.parse_args()

# ----------------------------
# Check image exists
# ----------------------------
if not os.path.isfile(args.image):
    print(f"Error: File not found: {args.image}")
    sys.exit(1)

# ----------------------------
# Prepare image
# ----------------------------
try:
    image_handle = lms.prepare_image(args.image)
except Exception as e:
    print(f"Error preparing image: {e}")
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
# Build chat and add message
# ----------------------------
chat = lms.Chat()
chat.add_user_message(args.prompt, images=[image_handle])

# ----------------------------
# Get prediction
# ----------------------------
try:
    prediction = model.respond(chat)
    # prediction is a list of message dicts
    print(prediction.content)
except Exception as e:
    print(f"Error getting prediction: {e}")
    sys.exit(1)
