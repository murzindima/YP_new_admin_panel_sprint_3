from config.settings import PostgresSettings, ElasticsearchSettings, StateSettings
from elasticsearch_loader import ElasticsearchLoader
from state import State, JsonFileStorage
from postgres_enricher import PostgresEnricher
from postgres_merger import PostgresMerger
from postgres_fetcher import PostgresFetcher
from transform import transform_to_json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logger.info("ETL process started")
    pg_config = PostgresSettings()
    es_config = ElasticsearchSettings()
    state_config = StateSettings()

    config = PostgresSettings(host=pg_config.host,
                              port=pg_config.port,
                              user=pg_config.user,  # "app",
                              password=pg_config.password,  # "123qwe",
                              dbname=pg_config.dbname)  # "movies_database",
    state_manager = State(storage=JsonFileStorage('state.json'))
    producer = PostgresFetcher(config, state_manager)
    producer.connect()

    # Obtaining updated data on persons
    updated_persons_data = producer.fetch_updated_persons()
    person_ids = [row[0] for row in updated_persons_data]
    logger.info(f"Number of updated person records fetched: {len(updated_persons_data)}")

    # Obtaining updated data on genres
    updated_genres_data = producer.fetch_updated_genres()
    genre_ids = [row[0] for row in updated_genres_data]
    logger.info(f"Number of updated genre records fetched: {len(updated_genres_data)}")

    # Obtaining updated data on film works
    updated_film_works_data = producer.fetch_updated_film_works()
    film_work_ids = [row[0] for row in updated_film_works_data]
    logger.info(f"Number of updated film work records fetched: {len(updated_film_works_data)}")

    producer.close()

    enricher = PostgresEnricher(config, state_manager)
    enricher.connect()

    # Additional related movies by person and genre
    additional_films_by_person = enricher.fetch_films_by_updated_persons(person_ids)
    additional_films_by_genre = enricher.fetch_films_by_updated_genres(genre_ids)

    # Merge all film works IDs
    film_work_ids += [row[0] for row in additional_films_by_person] + [row[0] for row in additional_films_by_genre]
    film_work_ids = list(set(film_work_ids))

    enricher.close()

    merger = PostgresMerger(config, state_manager)
    merger.connect()
    complete_film_data = merger.merge_film_data(film_work_ids)
    merger.close()

    transformed_data = transform_to_json(complete_film_data)
    es_loader = ElasticsearchLoader(es_config)
    es_loader.load_data(es_config.index, transformed_data)
    logger.info("ETL process completed successfully")
