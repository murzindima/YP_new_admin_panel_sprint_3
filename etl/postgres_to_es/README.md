# Overview

The service is designed to fetch data from a PostgreSQL database, 
transform it, and then load it into Elasticsearch for indexing. 
It supports state management through either a JSON file or Redis,
allowing flexibility and persistence across runs.

# Configuration

## Application Settings
**LOG_LEVEL**: Logging level for the application. Default is "INFO".

**BATCH_SIZE**: The number of records processed in each batch. Default is 100.

## PostgreSQL Settings
**PG_HOST**: Hostname of the PostgreSQL server. Default is "localhost".

**PG_PORT**: Port number for the PostgreSQL server. Default is 5432.

**PG_USER**: Username for PostgreSQL authentication.

**PG_PASSWORD**: Password for PostgreSQL authentication.

**PG_DBNAME**: Name of the database to connect to.

## Elasticsearch Settings
**ES_HOST**: Hostname of the Elasticsearch server. Default is `"localhost"".

**ES_PORT**: Port number for the Elasticsearch server. Default is 9200.

**ES_SCHEME**: Scheme for the Elasticsearch connection. Default is "http".

**ES_INDEX**: The Elasticsearch index where data will be loaded. Default is "movies".

## JSON File Storage Settings
**STATE_JSON_STORAGE_PATH**: Path to the JSON file used for state management. Default is "state.json".
## Redis Storage Settings
**STATE_REDIS_STORAGE_HOST**: Hostname of the Redis server. Default is "localhost".

**STATE_REDIS_STORAGE_PORT**: Port number for the Redis server. Default is 6379.

**STATE_REDIS_STORAGE_DB**: Redis database number. Default is 0.

## State Management Settings
STATE_STORAGE: Type of storage to be used for state management. Options are "json" or "redis".

# Running the Service
Ensure that PostgreSQL and Elasticsearch are running and accessible.
Set up the desired configuration parameters in the settings classes.
Start the ETL service. 
It will automatically connect to the specified PostgreSQL and Elasticsearch instances,
and manage state according to the provided settings.

# Customization

You can customize the service by modifying the settings in the respective configuration. 
This includes changing database connection details, Elasticsearch index, 
batch processing size, and the type of state management storage.

# Dependencies

PostgreSQL server (configured as per PostgresSettings).
Elasticsearch server (configured as per ElasticsearchSettings).
Redis server (optional, only if Redis is used for state management).

# Troubleshooting

If you encounter issues, check the log files based on the set log_level.
Ensure all services (PostgreSQL, Elasticsearch, Redis) 
are running and network configurations are correctly set in the service settings.
