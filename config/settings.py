import os
from dotenv import load_dotenv
from dataclasses import dataclass
from langchain_openai import ChatOpenAI, OpenAIEmbeddings

load_dotenv()

@dataclass(frozen=True)
class LLMProvider:
    """Connection details for a single LLM Provider."""
    api_key: str
    base_url: str
    default_model: str

    def chat_model(self, *, temperature:float = 0.3, max_tokens: int = 2048) -> ChatOpenAI:
        return ChatOpenAI(
            model = self.default_model,
            api_key = self.api_key,
            base_url = self.base_url,
            temperature=temperature,
            max_tokens=max_tokens
        )

    def embeddings_model(self) -> OpenAIEmbeddings:
        return OpenAIEmbeddings(
            model=self.default_model,
            api_key=self.api_key,
            base_url=self.base_url 
        )

# providers
OPENAI_PROVIDER = LLMProvider(
    api_key=os.getenv('OPENAI_API_KEY', '')
    base_url=os.getenv('OPENAI_BASE_URL', 'https://openrouter.ai/api/v1')
    default_model=os.getenv('OPENAI_MODEL', 'openai/gpt-4o-mini')
)

GEMINI_PROVIDER = LLMProvider(
    api_key=os.getenv('GEMINI_API_KEY', '')
    base_url=os.getenv("GEMINI_BASE_URL", "https://openrouter.ai/api/v1"),
    default_model=os.getenv("GEMINI_MODEL", "google/gemini-2.5-flash"),
)

PRIMARY_PROVIDER: LLMProvider = OPENAI_PROVIDER
VALIDATION_PROVIDER: LLMProvider = OPENAI_PROVIDER # GEMINI_PROVIDER
EMBEDDING_PROVIDER: LLMProvider = OPENAI_PROVIDER

# Database
DB_TYPE: str = os.getenv('DB_TYPE', 'sqlite')
DB_PATH: str = os.getenv('DB_PATH', '')

# Schema Cache
CACHE_DIR: str = os.path.expanduser("~/.sql_generator")
CACHE_FILE: str = os.path.join(CACHE_DIR, "schema_cache.json")
CACHE_TTL_HOURS: int = int(os.getenv(CACHE_TTL_HOURS, "24"))

# Pipleline
N_CANDIDATES: int = int(os.getenv('N_CANDIDATES', '3'))
MAX_RETRIES: int = int(os.getenv('MAX_RETRIES', '2'))
DISCOVERY_TOP_K: int = int(os.getenv('DISCOVERY_TOP_K', '10'))
KEYWORD_PRE_FILTER_TOP_N: int = int(os.getenv('KEYWORD_PRE_FILTER_TOP_N', '50'))

# Temperatures for Candidates
CANDIDATE_TEMPERATURES: list[float] = [0.3, 0.7, 0.5]
