import logging
from time import sleep
from psycopg2 import OperationalError, connect as pg_connect

from config.settings import PostgresSettings
from state import State

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PostgresBase:
    def __init__(self, pg_config: PostgresSettings, state: State):
        self.config = pg_config
        self.state = state
        self.conn = None
        self.cursor = None

    def connect(self):
        """ Подключение к PostgreSQL. """
        if self.conn is not None:
            return

        try:
            self.conn = pg_connect(
                host=self.config.host,
                port=self.config.port,
                user=self.config.user,
                password=self.config.password,
                dbname=self.config.dbname
            )
            self.cursor = self.conn.cursor()
            logger.info("Успешное подключение к PostgreSQL")
        except OperationalError as e:
            logger.error(f"Ошибка подключения: {e}")
            self.backoff_retry()

    def backoff_retry(self, retries=5, delay=1):
        """ Попытки повторного подключения с увеличением задержки. """
        for i in range(retries):
            try:
                sleep(delay * (2 ** i))
                self.connect()
                break
            except OperationalError:
                logger.warning(f"Попытка {i + 1} не удалась. Повторное подключение через {delay * (2 ** i)} секунд.")

    def close(self):
        """ Закрытие подключения к базе данных. """
        if self.conn:
            self.conn.close()
            logger.info("Подключение к PostgreSQL закрыто")

    def execute_query(self, query, params=None):
        try:
            self.cursor.execute(query, params)
            logger.debug(f"Executed query: {self.cursor.query.decode()}")
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise
