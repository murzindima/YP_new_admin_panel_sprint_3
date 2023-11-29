from postgres_common import PostgresBase
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PostgresEnricher(PostgresBase):
    def fetch_films_by_updated_persons(self, person_ids):
        """ Извлечение связанных фильмов для заданных ID персон. """
        logger.info("Fetching related films for given person IDs")
        if not person_ids:
            return []

        query = """
        SELECT fw.id, fw.updated_at
        FROM content.film_work fw
        LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
        WHERE pfw.person_id IN %s
        ORDER BY fw.updated_at
        LIMIT 1000;
        """
        self.cursor.execute(query, (tuple(person_ids),))
        rows = self.cursor.fetchall()
        logger.info(f"Fetched {len(rows)} related films for persons from PostgreSQL")
        return rows

    def fetch_films_by_updated_genres(self, genre_ids):
        """ Извлечение фильмов, связанных с обновлёнными жанрами. """
        logger.info("Fetching films affected by updated genres")
        if not genre_ids:
            return []

        query = """
        SELECT fw.id, fw.updated_at
        FROM content.film_work fw
        JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
        WHERE gfw.genre_id IN %s
        ORDER BY fw.updated_at
        LIMIT 1000;
        """
        self.cursor.execute(query, (tuple(genre_ids),))
        rows = self.cursor.fetchall()
        logger.info(f"Fetched {len(rows)} related films affected by updated genres from PostgreSQL")
        return rows
