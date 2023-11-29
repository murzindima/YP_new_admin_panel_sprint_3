from postgres_common import PostgresBase
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PostgresMerger(PostgresBase):
    def merge_film_data(self, film_work_ids):
        """ Сбор полной информации о фильмах по их ID. """
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
