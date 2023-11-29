import time
import logging

from config.settings import postgres_settings, elasticsearch_settings
from elasticsearch_loader import ElasticsearchLoader
from state import State, JsonFileStorage
from postgres_enricher import PostgresEnricher
from postgres_merger import PostgresMerger
from postgres_fetcher import PostgresFetcher
from transform import transform_to_json


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logger.info("ETL process initialising...")
    pg_config = postgres_settings
    es_config = elasticsearch_settings
    state_manager = State(storage=JsonFileStorage('state.json'))
    fetcher = PostgresFetcher(pg_config)
    enricher = PostgresEnricher(pg_config)
    merger = PostgresMerger(pg_config)
    es_loader = ElasticsearchLoader(es_config)

    while True:
        try:
            fetcher.connect()
            last_modified_person = state_manager.get_state('person_last_modified')
            updated_persons_data, new_last_modified_person = fetcher.fetch_updated_records('person', last_modified_person)
            person_ids = [row[0] for row in updated_persons_data]
            logger.debug(f"Number of updated person records fetched: {len(updated_persons_data)}")

            last_modified_genre = state_manager.get_state('genre_last_modified')
            updated_genres_data, new_last_modified_genre = fetcher.fetch_updated_records('genre', last_modified_genre)
            genre_ids = [row[0] for row in updated_genres_data]
            logger.debug(f"Number of updated genre records fetched: {len(updated_genres_data)}")

            last_modified_film_work = state_manager.get_state('film_work_last_modified')
            updated_film_works_data, new_last_modified_film_work = fetcher.fetch_updated_records('film_work', last_modified_film_work)
            film_work_ids = [row[0] for row in updated_film_works_data]
            logger.debug(f"Number of updated film work records fetched: {len(updated_film_works_data)}")
            fetcher.close()

            enricher.connect()
            # Additional related movies by person and genre
            additional_films_by_person = enricher.fetch_films_by_updated_persons(person_ids)
            additional_films_by_genre = enricher.fetch_films_by_updated_genres(genre_ids)
            # Merge all film works IDs
            film_work_ids += [row[0] for row in additional_films_by_person] + [row[0] for row in additional_films_by_genre]
            film_work_ids = list(set(film_work_ids))
            enricher.close()

            merger.connect()
            complete_film_data = merger.merge_film_data(film_work_ids)
            merger.close()

            transformed_data = transform_to_json(complete_film_data)
            if es_loader.load_data(es_config.index, transformed_data):
                if updated_persons_data:
                    state_manager.set_state("person_last_modified", new_last_modified_person)
                if updated_genres_data:
                    state_manager.set_state("genre_last_modified", new_last_modified_genre)
                if updated_film_works_data:
                    state_manager.set_state("film_work_last_modified", new_last_modified_film_work)
        except Exception as e:
            logger.error(f"ETL process encountered an error: {e}")

        time.sleep(60)
