from typing import Optional
from functools import lru_cache
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    LLM_PROVIDER: str = "gpt"
    
    OPENROUTER_API_KEY: str

    OPENAI_MODEL: str = "gpt-40-mini"
    OPENAI_API_KEY: Optional[str] = None
    GEMINI_API_KEY: Optional[str] = None

    class Config:
        env_file = '.env'
        case_sensitive = True

@lru_cache()
def get_settings() -> Settings:
    """
    Create and Cache settings instance,
    using the @lru_cache decorator, which means we'll load the .env once, and use the same settings everywhere.
    """

    return Settings()

settings = get_settings()
