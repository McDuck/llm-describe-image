from __future__ import annotations
import os
import sys
from typing import Any, Optional

from PIL import Image
import torch
from transformers import AutoProcessor, AutoModel
try:
    from transformers import BitsAndBytesConfig
except ImportError:
    BitsAndBytesConfig = None

# Import config
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..'))
from describe_media.config_loader import _config

from llms.base import LLMBackend


class HuggingFaceBackend(LLMBackend):
    def __init__(self):
        self.config = _config.get("huggingface", {})
        self.device = self.config.get("device", "auto")
        
        # Auto-detect GPU device
        if self.device == "auto":
            if torch.cuda.is_available():
                self.device = "cuda"
                self.gpu_type = "nvidia"
            elif hasattr(torch, 'xpu') and torch.xpu.is_available():
                self.device = "xpu"  # Intel GPU
                self.gpu_type = "intel"
            elif torch.version.hip:
                self.device = "cuda"  # AMD ROCm uses CUDA interface
                self.gpu_type = "amd"
            else:
                self.device = "cpu"
                self.gpu_type = "cpu"
        else:
            # Determine GPU type from explicitly set device
            if self.device == "cuda":
                if torch.version.hip:
                    self.gpu_type = "amd"
                else:
                    self.gpu_type = "nvidia"
            elif self.device == "xpu":
                self.gpu_type = "intel"
            else:
                self.gpu_type = "cpu"
        
        self.use_quantization = self.config.get("use_quantization", True)
        self.max_new_tokens = self.config.get("max_new_tokens", 512)
        self.temperature = self.config.get("temperature", 0.7)
        self.top_p = self.config.get("top_p", 0.9)
        
        self.model = None
        self.processor = None

    def bootstrap_server(self, auto_start: bool, sync_api_timeout_s: int = 600) -> bool:
        """Hugging Face doesn't need a server - models run locally."""
        return True

    def load_model(self, model_name: str, allow_cli_install: bool, context_size: int = 0) -> Any:
        """Load a Hugging Face vision model."""
        try:
            print(f"Loading Hugging Face model: {model_name}")
            print(f"Using device: {self.device} ({self.gpu_type})")
            print(f"CUDA available: {torch.cuda.is_available()}")
            if torch.cuda.is_available():
                print(f"CUDA device: {torch.cuda.get_device_name(0)}")
                print(f"Using ROCm (AMD): {bool(torch.version.hip)}")

            # Configure model loading
            model_kwargs = {
                "device_map": "auto",  # Always use auto device mapping
                "dtype": torch.float16,  # Use FP16 for efficiency
            }

            # Only add quantization on compatible GPU devices
            # AMD ROCm typically supports quantization
            if self.use_quantization and self.device != "cpu" and BitsAndBytesConfig:
                try:
                    quantization_config = BitsAndBytesConfig(
                        load_in_4bit=True,
                        bnb_4bit_compute_dtype=torch.float16,
                        bnb_4bit_use_double_quant=True,
                        bnb_4bit_quant_type="nf4"
                    )
                    model_kwargs["quantization_config"] = quantization_config
                    print(f"Using 4-bit quantization on {self.gpu_type}")
                except Exception as e:
                    print(f"Quantization not available: {e}. Continuing without quantization.")

            # Load model and processor
            self.model = AutoModel.from_pretrained(
                model_name,
                **model_kwargs
            )

            self.processor = AutoProcessor.from_pretrained(model_name)

            # Verify device placement
            if hasattr(self.model, 'device'):
                print(f"Model loaded on device: {self.model.device}")
            
            print(f"Model loaded successfully on {self.device} ({self.gpu_type})")
            return self.model

        except Exception as e:
            print(f"Failed to load model {model_name}: {e}")
            import traceback
            traceback.print_exc()
            return None

    def prepare_image(self, path: str) -> Image.Image:
        """Prepare image for inference by loading and converting to PIL Image."""
        try:
            image = Image.open(path).convert("RGB")
            return image
        except Exception as e:
            raise ValueError(f"Failed to load image {path}: {e}")

    def respond(self, model: Any, prompt: str, image_handle: Optional[Image.Image] = None) -> str:
        """Run inference with the model."""
        if model is None:
            raise ValueError("Model not loaded")

        try:
            if image_handle is not None:
                # Vision task - use chat format for Qwen models
                messages = [
                    {
                        "role": "user", 
                        "content": [
                            {"type": "image", "image": image_handle},
                            {"type": "text", "text": prompt}
                        ]
                    }
                ]
                
                # Apply chat template
                inputs = self.processor.apply_chat_template(
                    messages, 
                    add_generation_prompt=True, 
                    tokenize=True, 
                    return_dict=True, 
                    return_tensors="pt"
                ).to(self.device)

                # Generate response - use model's language_model if available
                generate_model = model.language_model if hasattr(model, 'language_model') else model
                with torch.no_grad():
                    generated_ids = generate_model.generate(
                        **inputs,
                        max_new_tokens=self.max_new_tokens,
                        do_sample=True,
                        temperature=self.temperature,
                        top_p=self.top_p,
                    )

                # Decode response
                generated_text = self.processor.batch_decode(generated_ids, skip_special_tokens=True)[0]
                return generated_text.strip()

            else:
                # Text-only task (for enhancement) - use chat format
                messages = [{"role": "user", "content": [{"type": "text", "text": prompt}]}]
                
                inputs = self.processor.apply_chat_template(
                    messages, 
                    add_generation_prompt=True, 
                    tokenize=True, 
                    return_dict=True, 
                    return_tensors="pt"
                ).to(self.device)

                generate_model = model.language_model if hasattr(model, 'language_model') else model
                with torch.no_grad():
                    generated_ids = generate_model.generate(
                        **inputs,
                        max_new_tokens=self.max_new_tokens,
                        do_sample=True,
                        temperature=self.temperature,
                        top_p=self.top_p,
                    )

                generated_text = self.processor.batch_decode(generated_ids, skip_special_tokens=True)[0]
                return generated_text.strip()

        except Exception as e:
            raise RuntimeError(f"Inference failed: {e}")

    def cleanup(self, model_loaded_by_script: bool, model_name: Optional[str], server_started_by_script: bool) -> None:
        """Cleanup resources."""
        if self.model is not None:
            del self.model
            self.model = None
        if self.processor is not None:
            del self.processor
            self.processor = None

        # Clear CUDA cache if available
        if torch.cuda.is_available():
            torch.cuda.empty_cache()
