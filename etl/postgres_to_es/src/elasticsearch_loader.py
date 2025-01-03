from elasticsearch import Elasticsearch, helpers
import logging

from config.settings import app_settings, ElasticsearchSettings

logging.basicConfig(level=app_settings.log_level.upper())
logger = logging.getLogger(__name__)


class ElasticsearchLoader:
    """
    A class for loading data into Elasticsearch.

    This class uses the Elasticsearch client to index data.
    It supports both single and batch loading of data into Elasticsearch.

    Attributes:
        es (Elasticsearch): An instance of the Elasticsearch client.
    """

    def __init__(self, es_config: ElasticsearchSettings):
        """
        Initializes ElasticsearchLoader with the configuration for the connection

        Args:
            es_config: Configuration for connecting to Elasticsearch.
        """
        self.es = Elasticsearch(
            hosts=[
                {
                    "host": es_config.host,
                    "port": es_config.port,
                    "scheme": es_config.scheme,
                }
            ]
        )

    def load_data(self, index: str, data: list) -> None:
        """
        Loads data into Elasticsearch using batch processing.

        This method takes a list of data and indexes it in Elasticsearch.
        Batch processing is used to increase performance.

        Args:
            index (str): The name of the Elasticsearch index into which the data will be loaded.
            data (list): The list of data to be indexed.
        """
        try:
            actions = [
                {"_index": index, "_id": record["id"], "_source": record}
                for record in data
            ]
            helpers.bulk(self.es, actions)
            if len(actions) > 0:
                logger.info(
                    "Successfully loaded %d documents to Elasticsearch", len(actions)
                )
        except Exception as e:
            logger.error("Failed to bulk index documents: %s", e)
