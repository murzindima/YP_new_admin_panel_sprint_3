from postgres_common import PostgresBase
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PostgresMerger(PostgresBase):
    """
        Class responsible for merging film data from various tables in PostgreSQL.

        Inherits from PostgresBase to utilize common database functionalities.
        This class manages the aggregation of comprehensive film data,
        including details related to persons and genres associated with the film works.
    """
    def merge_film_data(self, film_work_ids):
        """
        Merges complete film data for a given list of film work IDs.

        Args:
            film_work_ids (list): A list of film work IDs for which the comprehensive data needs to be fetched.

        Returns:
            list: A list of tuples containing aggregated film data.
        """
        logger.info("Merging complete film data for given film work IDs")
        if not film_work_ids:
            return []

        query = """
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
        WHERE fw.id IN %s;
        """
        self.cursor.execute(query, (tuple(film_work_ids),))
        rows = self.cursor.fetchall()
        logger.info(f"Fetched {len(rows)} complete film records from PostgreSQL")
        return rows
