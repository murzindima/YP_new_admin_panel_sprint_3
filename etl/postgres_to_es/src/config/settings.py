from pydantic_settings import BaseSettings, SettingsConfigDict


class AppSettings(BaseSettings):
    log_level: str = "INFO"
    batch_size: int = 100


class PostgresSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix='pg_')
    host: str = "localhost"
    port: int = 5432
    user: str = "your_user"
    password: str = "your_password"
    dbname: str = "your_db"


class ElasticsearchSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix='es_')
    host: str = "localhost"
    port: int = 9200
    scheme: str = "http"
    index: str = "movies"


class JsonFileStorageSettings(BaseSettings):
    path: str = "state.json"


class RedisStorageSettings(BaseSettings):
    host: str = "localhost"
    port: int = 6379
    db: int = 0


class StateSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix='state_')
    storage: str = "json"
    json_storage: JsonFileStorageSettings = JsonFileStorageSettings()
    redis_storage: RedisStorageSettings = RedisStorageSettings()


postgres_settings = PostgresSettings()
elasticsearch_settings = ElasticsearchSettings()
state_settings = StateSettings()
app_settings = AppSettings()
