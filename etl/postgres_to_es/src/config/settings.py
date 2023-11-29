from pydantic_settings import BaseSettings, SettingsConfigDict


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


class StateSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix='state_')
    genre_file_path: str = "genre_state.txt"
    person_file_path: str = "person_state.txt"
    film_work_file_path: str = "film_work_state.txt"


postgres_settings = PostgresSettings()
elasticsearch_settings = ElasticsearchSettings()
state_settings = StateSettings()
