from postgres_common import PostgresBase
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PostgresFetcher(PostgresBase):
    """
        Class responsible for fetching updated data from the PostgreSQL database.

        Inherits from PostgresBase to utilize common database functionalities and manages fetching of updated data
        in specific tables like person, genre, and film_work.
    """
    def fetch_updated_records(self, table_name, last_modified, limit=100):
        """
        Fetches updated records from the specified table in the database.

        Args:
            table_name (str): Name of the table to fetch records from.
            last_modified (str): The last modified timestamp to fetch records after.
            limit (int, optional): Maximum number of records to fetch.
            Defaults to 100.

        Returns:
            tuple: A tuple containing a list of updated records and the timestamp of the last updated record.
        """
        query_date = last_modified or '1900-01-01'
        query = f"""
        SELECT id, updated_at
        FROM content.{table_name}
        WHERE updated_at > %s
        ORDER BY updated_at
        LIMIT %s;
        """
        self.cursor.execute(query, (query_date, limit))
        rows = self.cursor.fetchall()
        logger.debug(f"Fetched {len(rows)} updated records from {table_name} table in PostgreSQL")
        return rows, str(rows[-1][1]) if rows else None
