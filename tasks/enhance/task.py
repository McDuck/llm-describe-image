import os
import sys
from typing import Optional, Tuple, List
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

# Add local llms directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'llm'))

from tasks.task import Task
from llms import get_backend
from llms.base import LLMBackend


class EnhanceTask(Task[Tuple[str, str, List[str]], Tuple[str, str]]):
    """
    Use LLM to enhance description based on context from nearby images.
    
    Input: (image_path, original_description, context_descriptions)
    Output: (image_path, enhanced_description)
    """
    
    def __init__(
        self,
        maximum: int = 1,
        model_name: Optional[str] = None,
        prompt: Optional[str] = None,
        backend_name: Optional[str] = None,
        input_dir: Optional[str] = None,
        context_template: Optional[str] = None,
        context_item_template: Optional[str] = None,
        context_item_max_length: Optional[int] = None,
        max_context_in_prompt: Optional[int] = None,
        model_context_length: Optional[int] = None,
    ) -> None:
        super().__init__(maximum, input_dir=input_dir)
        self.model_name: Optional[str] = model_name
        self.backend_name: Optional[str] = backend_name
        self.backend: Optional[LLMBackend] = None
        self.model = None
        self.model_context_length: Optional[int] = model_context_length
        
        self.prompt: Optional[str] = prompt
        
        # Context formatting configuration
        from config_loader import (
            DEFAULT_CONTEXT_TEMPLATE,
            DEFAULT_CONTEXT_ITEM_TEMPLATE,
            DEFAULT_CONTEXT_ITEM_MAX_LENGTH,
            DEFAULT_MAX_CONTEXT_IN_PROMPT
        )
        self.context_template: str = context_template or DEFAULT_CONTEXT_TEMPLATE
        self.context_item_template: str = context_item_template or DEFAULT_CONTEXT_ITEM_TEMPLATE
        self.context_item_max_length: int = context_item_max_length or DEFAULT_CONTEXT_ITEM_MAX_LENGTH
        self.max_context_in_prompt: int = max_context_in_prompt or DEFAULT_MAX_CONTEXT_IN_PROMPT
    
    def load(self) -> None:
        """Load the model and backend. Called by worker thread at start."""
        from config_loader import DEFAULT_MODEL_CONTEXT_LENGTH
        
        self.backend = get_backend(self.backend_name)
        if self.backend:
            # Load with configured context window for handling context-rich prompts
            context_size = self.model_context_length or DEFAULT_MODEL_CONTEXT_LENGTH
            self.model = self.backend.load_model(self.model_name, allow_cli_install=False, context_size=context_size)
        
        if not self.model:
            raise Exception(f"Failed to load context enhancement model: {self.model_name}")
    
    def unload(self) -> None:
        """Unload the model. Called by worker thread at end."""
        if self.backend and hasattr(self.backend, 'cleanup'):
            self.backend.cleanup(
                model_loaded_by_script=True,
                model_name=self.model_name,
                server_started_by_script=False
            )
        self.model = None
        self.backend = None
    
    def execute(self, item: Tuple[str, str, List[str]]) -> Tuple[str, str]:
        """
        Enhance description using LLM with context.
        Args: (image_path, original_description, context_descriptions)
        Returns: (image_path, enhanced_description)
        """
        image_path, original_desc, context_descs = item
        
        try:
            if not self.backend or not self.model:
                raise Exception("Backend or model not configured")
            
            # Build context section using templates from config
            context_section = ""
            if context_descs:
                items_text = ""
                for i, ctx in enumerate(context_descs[:self.max_context_in_prompt], 1):
                    # Truncate context item if needed
                    truncated = ctx[:self.context_item_max_length] + "..." if len(ctx) > self.context_item_max_length else ctx
                    items_text += self.context_item_template.format(
                        number=i,
                        description=truncated
                    )
                
                context_section = self.context_template.format(items=items_text)
            
            # Format prompt with placeholders
            try:
                full_prompt = self.prompt.format(
                    context_section=context_section,
                    original_description=original_desc
                )
            except (KeyError, ValueError):
                # If template has no placeholders or formatting fails, use as-is
                full_prompt = self.prompt
            
            # Run LLM inference (text-only, no image)
            enhanced_desc = self.backend.respond(self.model, full_prompt)
            
            return (image_path, enhanced_desc)
            
        except Exception as e:
            # Show relative path in error
            rel_path = image_path
            if self.input_dir and image_path.startswith(self.input_dir):
                try:
                    rel_path = os.path.relpath(image_path, self.input_dir)
                except (ValueError, TypeError):
                    pass
            raise Exception(f"Enhancement failed for {rel_path}: {str(e)}")
