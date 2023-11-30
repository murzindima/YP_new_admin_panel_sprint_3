import logging
from typing import Any

from config.settings import app_settings

logging.basicConfig(level=app_settings.log_level.upper())
logger = logging.getLogger(__name__)


def transform_to_json(rows: list[tuple[Any]]) -> list[dict[str, Any]]:
    """
    Transforms data rows into JSON format suitable for Elasticsearch.

    Args:
        rows (list): A list of rows containing film work data.

    Returns:
        list: A list of dictionaries where each dictionary represents a film record in JSON format.
    """
    logger.debug(
        "Starting data transformation into JSON format suitable for Elasticsearch..."
    )
    films = {}

    for row in rows:
        film_id = row.fw_id
        role = row.role
        person_id = row.id
        full_name = row.full_name
        genre_name = row.name

        # Create a new movie record if it has not been added yet
        logger.debug("Processing film ID: %s", film_id)
        if film_id not in films:
            films[film_id] = {
                "id": film_id,
                "imdb_rating": row.rating,
                "genre": [],
                "title": row.title,
                "description": row.description,
                "actors_names": [],
                "writers_names": [],
                "actors": [],
                "writers": [],
                "director": [],
            }

        # Add a genre if one has not already been added
        if genre_name and genre_name not in films[film_id]["genre"]:
            films[film_id]["genre"].append(genre_name)

        # Add an actor if one has not already been added
        if role == "actor" and person_id not in [
            actor["id"] for actor in films[film_id]["actors"]
        ]:
            films[film_id]["actors_names"].append(full_name)
            films[film_id]["actors"].append({"id": person_id, "name": full_name})

        # Add a writer if one has not already been added
        if role == "writer" and person_id not in [
            writer["id"] for writer in films[film_id]["writers"]
        ]:
            films[film_id]["writers_names"].append(full_name)
            films[film_id]["writers"].append({"id": person_id, "name": full_name})

    logger.debug(
        "Data transformation into JSON format suitable for Elasticsearch completed"
    )
    transformed_count = len(films)
    logger.debug("Transformed %d film records for Elasticsearch", transformed_count)
    return list(films.values())
