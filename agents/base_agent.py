"""
BASE AGNET - Abstract Interface for all LLM agents.

hanldes:
- Async LLM calls via ChatOpenAI (langhcain-openai)
- provider-aware: each agent recieves an LLM provider
- token usage logging

Each agent implements:
- build_prompt()
- parse_response()
"""

import logging
from abc import ABC, abstractmethod
from langchain_openai import ChatOpenAI
from config.settings import LLMProvider, PRIMARY_PROVIDER
from langchain_core.messages import BaseMessage, HumanMessage, SystemMessage

logger = logging.getLogger(__name__)

def _to_langchain_messages(messages: list[dict[str, str]]) -> list[BaseMessage]:
    mapping = {"system": SystemMessage, "user": HumanMessage}
    return [mapping.get(m["role"], HumanMessage)(content=m["content"]) for m in messages]

class BaseAgent(ABC):
    def __init__(self, provider: LLMProvider = PRIMARY_PROVIDER) -> None:
        self.provider = provider
        self.model_name = provider.default_model

    @abstractmethod
    def build_prompt(self, *args, **kwargs) -> list[dict[str, str]]:
        raise NotImplementedError("Abstract Method build_prompt not implemented")

    @abstractmethod
    def parse_response(self, raw: str) -> Any:
        raise NotImplementedError("Abstract Method ParseResponse not implemented")
    
    def _get_llm(self, temperature: float=0.3, max_tokens: int = 2048) -> ChatOpenAI:
        return self.provider.chat_model(temperature=temperature, max_tokens=max_tokens)
    
    async def call_llm(
            self,
            messages: list[dict[str, str]],
            temperature: float = 0.3,
            max_tokens: int = 2048
    ) -> str:
        logger.debug(
            "%s -> LLM (model=%s, temp=%.1f, msgs=%d)", self.__class__.__name__, self.model_name, temperature, len(messages)
        )

        llm = self._get_llm(temperature=temperature, max_tokens=max_tokens)
        lc_messages = _to_langchain_messages(messages)

        response = await llm.ainvoke(lc_messages)

        if response.usages_metadata:
            logger.debug("%s ← LLM (prompt=%d, completion=%d tokens)",
                self.__class__.__name__,
                response.usage_metadata.get("input_tokens", 0),
                response.usage_metadata.get("output_tokens", 0),
            )

        return response.content.strip()

    async def execute(self, *args, **kwargs) -> Any:
        messages = self.build_prompt(*args, **kwargs)
        raw = await self.call_llm(
            messages,
            temperature=kwargs.get("temperature", 0.3),
            max_tokens=kwargs.get("max_tokens", 2048)
        )
        return self.parse_response(raw)
