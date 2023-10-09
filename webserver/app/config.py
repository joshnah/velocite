from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    cassandra_host: str
    cassandra_port: int

    model_config = SettingsConfigDict(env_file=".env")