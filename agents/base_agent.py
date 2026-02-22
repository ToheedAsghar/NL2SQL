import logging
from typing import Any
from __future__ import annotations
from abc import ABC, abstractmethod

from openai import AsyncOpenAI
from config.settings import OPENAI_API_KEY, OPENAI_MODEL

logger = logging.getLogger(__name__)

class BaseAgent(ABC):
    def __init__(self, model: str = OPENAI_MODEL) -> None:
        self.client = AsyncOpenAI(api_key=OPENAI_API_KEY)
        self.model = model

        @abstractmethod
        def build_prompt(self, *args, **kwargs) -> list[dict[str, str]]:
            """Returns OpenAI message list (role/content pairs)"""
            ...
        
        @abstractmethod
        def parse_response(self, raw: str) -> Any:
            """Parse raw LLM Text into a typed result"""
            ...

        async def call_llm(
                self,
                messages: list[dict[str, str]],
                temperature: float = 0.3,
                max_tokens: int = 2048
        ) -> str:
            logger.debug('[INFO][%s -> LLM (model=%s, temp=%.1f, msgs=%d)]', self.__class__, self.model, temperature, len(messages))

            response = await self.client.chat.completions.create(
                
            )
