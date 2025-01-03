import logging
from datetime import date
from time import sleep

from psycopg import (
    OperationalError,
    connect as pg_connect,
    InterfaceError,
    DatabaseError,
)
from psycopg.rows import namedtuple_row

from config.settings import PostgresSettings, app_settings

logging.basicConfig(level=app_settings.log_level.upper())
logger = logging.getLogger(__name__)


class PostgresFetcher:
    """
    Base class for working with PostgreSQL.

    The basic functionality includes connecting to the database, executing queries
    and closing the connection.

    Attributes:
        config (PostgresSettings): The configuration for connecting to PostgreSQL.
        conn: Instance of the database connection.
        cursor: The cursor for executing queries.
    """

    def __init__(self, pg_config: PostgresSettings):
        """
        Initialization of PostgresBase with specified configuration and state.

        Args:
            pg_config (PostgresSettings): The configuration for connecting to PostgreSQL.
        """
        self.config = pg_config
        self.conn = None
        self.cursor = None
        self.limit = app_settings.batch_size

    def connect(self) -> None:
        """
        Establishes a connection to PostgreSQL.

        If the connection is already established, the method does nothing.
        If the connection fails, the reconnect procedure is initiated.
        """
        if self.conn is not None:
            try:
                self.conn.close()
            except Exception as e:
                logger.error("Error closing broken connection: %s", e)
            self.conn = None
            self.cursor = None

        try:
            self.conn = pg_connect(
                host=self.config.host,
                port=self.config.port,
                user=self.config.user,
                password=self.config.password,
                dbname=self.config.dbname,
            )
            self.cursor = self.conn.cursor(row_factory=namedtuple_row)
            # self.cursor = ClientCursor(self.conn)
            logger.debug("Successful connection to PostgreSQL")
        except OperationalError as e:
            logger.error("Connection error: %s", e)
            self.backoff_retry()

    def backoff_retry(self, retries: int = 5, delay: int = 1) -> None:
        """
        Repeated connection attempts with exponential delay.

        Args:
            retries (int): Maximum number of connection retries.
            delay (int): Initial delay between attempts (in seconds).
        """
        for i in range(retries):
            try:
                sleep(delay * (2**i))
                self.connect()
                break
            except OperationalError:
                logger.warning(
                    "Attempt %d failed. Reconnect after %d seconds.",
                    i + 1,
                    delay * (2**i),
                )

    def close(self) -> None:
        """
        Closes the connection to the database.

        If the connection is active, it will be closed.
        """
        if self.conn:
            self.conn.close()
            logger.debug("Connection to PostgreSQL closed")

    def handle_db_disconnection(self) -> None:
        try:
            self.conn.close()
        except Exception as e:
            logger.error("Error closing broken connection: %s", e)
        finally:
            self.conn = None
            self.cursor = None

    def execute_query(self, query: str, params: tuple | None = None) -> None:
        """
        Executes an SQL query in PostgreSQL.

        Args:
            query (str): The text of the SQL query.
            params (tuple, optional): Parameters for the SQL query.
        """
        logger.debug("Query: %s", query)
        try:
            self.cursor.execute(query, params)
        except (OperationalError, InterfaceError, DatabaseError) as e:
            logger.error("Database error: %s", e)
            self.handle_db_disconnection()
            self.backoff_retry()
            raise
        except Exception as e:
            logger.error("Query execution failed: %s", e)
            raise

    def fetch_updated_records(
        self, table_name: str, last_modified: str | None = None
    ) -> tuple:
        """
        Fetches updated records from the specified table in the database.

        Args:
            table_name (str): Name of the table to fetch records from.
            last_modified (str): The last modified timestamp to fetch records after.
            Defaults to 100.

        Returns:
            tuple: A tuple containing a list of updated records and the timestamp of the last updated record.
        """
        limit = self.limit
        query_date = last_modified or str(date.min)
        logger.debug(
            "Fetching updated records for %s table from %s", table_name, query_date
        )
        query = f"""
                SELECT id, updated_at
                FROM content.{table_name}
                WHERE updated_at > %s
                ORDER BY updated_at
                LIMIT %s;
                """
        self.execute_query(query, (query_date, limit))
        rows = self.cursor.fetchall()
        # logger.debug(rows)
        logger.debug(
            "Fetched %d updated records from %s table in PostgreSQL",
            len(rows),
            table_name,
        )
        return rows, str(rows[-1].updated_at) if rows else None

    def fetch_films_by_updated_persons(self, person_ids: list) -> list:
        """
        Fetches films related to the given person IDs.

        Args:
            person_ids (list): A list of person IDs to fetch related films.

        Returns:
            list: A list of tuples containing film IDs and their updated timestamps.
        """
        logger.debug("Fetching related films for given person IDs")
        if not person_ids:
            return []

        limit = self.limit
        formatted_person_ids = ", ".join(f"'{person_id}'" for person_id in person_ids)
        query = f"""
        SELECT fw.id, fw.updated_at
        FROM content.film_work fw
        LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
        WHERE pfw.person_id IN ({formatted_person_ids})
        ORDER BY fw.updated_at
        LIMIT %s;
        """
        self.execute_query(query, (limit,))
        rows = self.cursor.fetchall()
        # logger.debug(rows)
        logger.debug("Fetched %d related films for persons from PostgreSQL", len(rows))
        return rows

    def fetch_films_by_updated_genres(self, genre_ids: list) -> list:
        """
        Fetches films related to the updated genres.

        Args:
            genre_ids (list): A list of genre IDs to fetch related films.

        Returns:
            list: A list of tuples containing film IDs and their updated timestamps.
        """
        logger.debug("Fetching films affected by updated genres")
        if not genre_ids:
            return []

        limit = self.limit
        formatted_genre_ids = ", ".join(f"'{genre_id}'" for genre_id in genre_ids)
        query = f"""
        SELECT fw.id, fw.updated_at
        FROM content.film_work fw
        JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
        WHERE gfw.genre_id IN ({formatted_genre_ids})
        ORDER BY fw.updated_at
        LIMIT %s;
        """
        self.execute_query(query, (limit,))
        rows = self.cursor.fetchall()
        # logger.debug(rows)
        logger.debug(
            "Fetched %d related films affected by updated genres from PostgreSQL",
            len(rows),
        )
        return rows

    def merge_film_data(self, film_work_ids: list) -> list:
        """
        Merges complete film data for a given list of film work IDs.

        Args:
            film_work_ids (list): A list of film work IDs for which the comprehensive data needs to be fetched.

        Returns:
            list: A list of tuples containing aggregated film data.
        """
        logger.debug("Merging complete film data for given film work IDs")
        if not film_work_ids:
            return []

        formatted_film_work_ids = ", ".join(
            f"'{film_work_id}'" for film_work_id in film_work_ids
        )
        query = f"""
        SELECT
            fw.id as fw_id, 
            fw.title, 
            fw.description, 
            fw.rating, 
            fw.type, 
            fw.created_at, 
            fw.updated_at, 
            pfw.role, 
            p.id, 
            p.full_name,
            g.name
        FROM content.film_work fw
        LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
        LEFT JOIN content.person p ON p.id = pfw.person_id
        LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
        LEFT JOIN content.genre g ON g.id = gfw.genre_id
        WHERE fw.id IN ({formatted_film_work_ids});
        """
        self.execute_query(query)
        rows = self.cursor.fetchall()
        # logger.debug(rows)
        logger.debug("Fetched %d complete film records from PostgreSQL", len(rows))
        return rows
