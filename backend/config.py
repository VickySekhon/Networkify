from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    neo4j_uri: str = "bolt://localhost:7687"
    neo4j_user: str = ""
    neo4j_password: str = ""
    openai_api_key: str = ""
    gemini_api_key: str = ""
    scrapfly_api_key: str = ""
    logo_dev_token: str = ""
    auth0_domain: str
    auth0_client_id: str
    auth0_client_secret: str
    app_secret_key: str
    frontend_url: str = "http://localhost:3000"

    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore"
    )

settings = Settings()
