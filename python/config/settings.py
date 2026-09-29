"""应用配置管理，通过环境变量加载。"""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # LLM
    openai_api_key: str = ""
    openai_model: str = "gpt-4o"
    openai_base_url: str = ""
    # 推理模型思考预算控制（如 DeepSeek v4 系列设为 none 禁用思考，缩短延迟）；
    # OpenAI 官方等不支持该参数的 provider 留空即可
    llm_reasoning_effort: str = ""
    minimax_api_key: str = ""
    minimax_model: str = "MiniMax-M2.7"

    # Database
    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/edu_agent"
    redis_url: str = "redis://localhost:6379/0"

    # Server
    api_port: int = 8000
    log_level: str = "INFO"

    model_config = {"env_file": "../.env", "env_file_encoding": "utf-8", "extra": "ignore"}


settings = Settings()
