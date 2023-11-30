import time
import logging

from redis import Redis

from config.settings import (
    postgres_settings,
    elasticsearch_settings,
    state_settings,
    app_settings,
)
from elasticsearch_loader import ElasticsearchLoader
from state_manager import State, JsonFileStorage, RedisStorage
from postgres_fetcher import PostgresFetcher
from transform import transform_to_json


logging.basicConfig(level=app_settings.log_level.upper())
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logger.info("ETL process initialising...")
    pg_config = postgres_settings
    es_config = elasticsearch_settings

    if state_settings.storage == "json":
        state_manager = State(storage=JsonFileStorage("state.json"))
    elif state_settings.storage == "redis":
        state_manager = State(
            storage=RedisStorage(
                Redis(
                    host=state_settings.redis.host,
                    port=state_settings.redis.port,
                    db=state_settings.redis.db,
                )
            )
        )
    else:
        raise ValueError("Unknown type of state storage")
    pg_fetcher = PostgresFetcher(pg_config)
    es_loader = ElasticsearchLoader(es_config)
    pg_fetcher.connect()
    last_modified_person = state_manager.get_state("person_last_modified")
    last_modified_genre = state_manager.get_state("genre_last_modified")
    last_modified_film_work = state_manager.get_state("film_work_last_modified")
    logger.info("ETL process started")

    while True:
        try:
            # Fetch updated records for person, genre and film work
            last_modified_person = state_manager.get_state("person_last_modified")
            (
                updated_persons_data,
                new_last_modified_person,
            ) = pg_fetcher.fetch_updated_records("person", last_modified_person)
            person_ids = [row[0] for row in updated_persons_data]
            # Fetch updated records for genre
            last_modified_genre = state_manager.get_state("genre_last_modified")
            (
                updated_genres_data,
                new_last_modified_genre,
            ) = pg_fetcher.fetch_updated_records("genre", last_modified_genre)
            genre_ids = [row[0] for row in updated_genres_data]
            # Fetch updated records for film work
            last_modified_film_work = state_manager.get_state("film_work_last_modified")
            (
                updated_film_works_data,
                new_last_modified_film_work,
            ) = pg_fetcher.fetch_updated_records("film_work", last_modified_film_work)
            film_work_ids = [row[0] for row in updated_film_works_data]
            # Additional related movies by person and genre
            additional_films_by_person = pg_fetcher.fetch_films_by_updated_persons(
                person_ids
            )
            additional_films_by_genre = pg_fetcher.fetch_films_by_updated_genres(
                genre_ids
            )
            # Merge all film works IDs
            film_work_ids += [row[0] for row in additional_films_by_person] + [
                row[0] for row in additional_films_by_genre
            ]
            film_work_ids = list(set(film_work_ids))
            complete_film_data = pg_fetcher.merge_film_data(film_work_ids)
            # Transform and load data
            transformed_data = transform_to_json(complete_film_data)
            es_loader.load_data(es_config.index, transformed_data)
            # Ensure new_last_modified_* variables have values before updating state
            if new_last_modified_person:
                state_manager.set_state(
                    "person_last_modified", new_last_modified_person
                )
            if new_last_modified_genre:
                state_manager.set_state("genre_last_modified", new_last_modified_genre)
            if new_last_modified_film_work:
                state_manager.set_state(
                    "film_work_last_modified", new_last_modified_film_work
                )
            if (
                not updated_persons_data
                and not updated_genres_data
                and not updated_film_works_data
            ):
                logger.info("no new data to process")
                time.sleep(1)
        except Exception as e:
            logger.error("ETL process encountered an error: %s", e)

        time.sleep(1)
