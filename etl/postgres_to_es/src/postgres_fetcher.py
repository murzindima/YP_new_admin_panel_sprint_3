from postgres_common import PostgresBase
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PostgresFetcher(PostgresBase):
    def fetch_updated_persons(self):
        """ Извлечение обновлённых данных. """
        last_modified = self.state.get_state("person_last_modified")
        if last_modified is None:
            self.cursor.execute(
                "SELECT id, updated_at FROM content.person ORDER BY updated_at LIMIT 1000"
            )
        else:
            self.cursor.execute(
                "SELECT id, updated_at FROM content.person WHERE updated_at > %s ORDER BY updated_at LIMIT 1000",
                (last_modified,)
            )
        rows = self.cursor.fetchall()
        logger.info(f"Fetched {len(rows)} updated persons from PostgreSQL")
        return rows, str(rows[-1][1]) if rows else None

    def fetch_updated_genres(self):
        """ Извлечение обновлённых жанров. """
        last_modified = self.state.get_state("genre_last_modified")
        query = """
        SELECT id, updated_at
        FROM content.genre
        WHERE updated_at > %s
        ORDER BY updated_at
        LIMIT 1000;
        """
        self.cursor.execute(query, (last_modified,))
        rows = self.cursor.fetchall()
        logger.info(f"Fetched {len(rows)} updated genres from PostgreSQL")
        return rows

    def fetch_updated_film_works(self):
        """ Извлечение обновлённых данных о фильмах. """
        last_modified = self.state.get_state("film_work_last_modified")
        if last_modified is None:
            self.cursor.execute(
                "SELECT id, updated_at FROM content.film_work ORDER BY updated_at LIMIT 1000"
            )
        else:
            self.cursor.execute(
                "SELECT id, updated_at FROM content.film_work WHERE updated_at > %s ORDER BY updated_at LIMIT 1000",
                (last_modified,)
            )
        rows = self.cursor.fetchall()
        logger.info(f"Fetched {len(rows)} updated film works from PostgreSQL")
        return rows

