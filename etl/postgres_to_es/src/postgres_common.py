import logging
from time import sleep
from psycopg2 import OperationalError, connect as pg_connect

from config.settings import PostgresSettings
from state import State

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PostgresBase:
    """
        Base class for working with PostgreSQL.

        The basic functionality includes connecting to the database, executing queries
        and closing the connection.

        Attributes:
            config (PostgresSettings): The configuration for connecting to PostgreSQL.
            state: An instance of the class to manage the state of the ETL process.
            conn: Instance of the database connection.
            cursor: The cursor for executing queries.
    """
    def __init__(self, pg_config: PostgresSettings, state: State):
        """
            Initialization of PostgresBase with specified configuration and state.

            Args:
                pg_config (PostgresSettings): The configuration for connecting to PostgreSQL.
                state (State): Class instance to manage the state of the ETL process.
        """
        self.config = pg_config
        self.state = state
        self.conn = None
        self.cursor = None

    def connect(self):
        """
            Establishes a connection to PostgreSQL.

            If the connection is already established, the method does nothing.
            If the connection fails, the reconnect procedure is initiated.
        """
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
            logger.debug("Successful connection to PostgreSQL")
        except OperationalError as e:
            logger.error(f"Connection error: {e}")
            self.backoff_retry()

    def backoff_retry(self, retries=5, delay=1):
        """
            Repeated connection attempts with exponential delay.

            Args:
                retries (int): Maximum number of connection retries.
                delay (int): Initial delay between attempts (in seconds).
        """
        for i in range(retries):
            try:
                sleep(delay * (2 ** i))
                self.connect()
                break
            except OperationalError:
                logger.warning(f"Attempt {i + 1} failed. Reconnect after {delay * (2 ** i)} seconds.")

    def close(self):
        """
            Closes the connection to the database.

            If the connection is active, it will be closed.
        """
        if self.conn:
            self.conn.close()
            logger.debug("Connection to PostgreSQL closed")

    def execute_query(self, query, params=None):
        """
            Executes an SQL query in PostgreSQL.

            Args:
                query (str): The text of the SQL query.
                params (tuple, optional): Parameters for the SQL query.
        """
        try:
            self.cursor.execute(query, params)
            logger.debug(f"Executed query: {self.cursor.query.decode()}")
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise
