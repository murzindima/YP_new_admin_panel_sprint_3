from elasticsearch import Elasticsearch
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ElasticsearchLoader:
    def __init__(self, es_config):
        self.es = Elasticsearch(
            hosts=[{
                'host': es_config.host,
                'port': es_config.port,
                'scheme': es_config.scheme,
            }]
        )

    def load_data(self, index, data):
        successful_loads = 0
        for record in data:
            try:
                self.es.index(index=index, id=record['id'], body=record)
                successful_loads += 1
            except Exception as e:
                logger.error(f"Failed to index document: {e}")
        logger.info(f"Successfully loaded {successful_loads} documents to Elasticsearch")
