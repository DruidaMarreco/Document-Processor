from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="DOC_PROCESSOR_", env_file=".env")

    host: str = "0.0.0.0"
    port: int = 8000
    log_level: str = "INFO"
    log_file: str = "logs/document_processor.jsonl"

    # LLM settings
    llm_model: str = "claude-haiku-4-5"
    llm_enabled: bool = True
    llm_confidence_threshold: float = 0.50
    llm_max_input_chars: int = 4_000
    llm_vision_enabled: bool = True

    # Retention policy: 0 = disabled, N = delete results older than N days
    retention_days: int = 0


settings = Settings()
