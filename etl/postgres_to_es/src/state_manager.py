import logging
import abc
import json
from typing import Any
from redis import Redis

from config.settings import app_settings

logging.basicConfig(level=app_settings.log_level.upper())
logger = logging.getLogger(__name__)


class BaseStorage(abc.ABC):
    """
    An abstract base class for storage mechanisms. Defines methods for saving and retrieving state data.
    """

    @abc.abstractmethod
    def save_state(self, state: dict[str, Any]) -> None:
        """
        Abstract method to save state data.

        Args:
            state (Dict[str, Any]): The state data to be saved.
        """
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> dict[str, Any]:
        """
        Abstract method to retrieve state data.

        Returns:
            Dict[str, Any]: The retrieved state data.
        """
        pass


class JsonFileStorage(BaseStorage):
    """
    A concrete implementation of BaseStorage that stores state in a JSON file.
    """

    def __init__(self, file_path: str) -> None:
        """
        Initializes the JsonFileStorage with a file path.

        Args:
            file_path (str): Path to the JSON file used for storage.
        """
        self.file_path = file_path

    def save_state(self, state: dict[str, Any]) -> None:
        """
        Saves state data to a JSON file.

        Args:
            state (Dict[str, Any]): The state data to be saved.
        """
        with open(self.file_path, "w") as file:
            json.dump(state, file)

    def retrieve_state(self) -> dict[str, Any]:
        """
        Retrieves state data from a JSON file.

        Returns:
            Dict[str, Any]: The retrieved state data.
        """
        try:
            with open(self.file_path, "r") as file:
                return json.load(file)
        except FileNotFoundError:
            logger.error("State file %s does not exist", self.file_path)
            return {}
        except json.JSONDecodeError:
            logger.error("State file %s is empty", self.file_path)
            return {}
        except Exception as e:
            logger.error("Failed to retrieve state: %s", e)
            return {}


class RedisStorage(BaseStorage):
    """
    A concrete implementation of BaseStorage that stores state in a Redis database.
    """

    def __init__(self, redis_adapter: Redis):
        """
        Initializes the RedisStorage with a Redis adapter.

        Args:
            redis_adapter (Redis): The Redis adapter for connecting to the Redis database.
        """
        self.redis_adapter = redis_adapter

    def save_state(self, state: dict[str, Any]) -> None:
        """
        Saves state data to a Redis database.

        Args:
            state (Dict[str, Any]): The state data to be saved.
        """
        for key, value in state.items():
            self.redis_adapter.set(key, json.dumps(value))

    def retrieve_state(self) -> dict[str, Any]:
        """
        Retrieves state data from a Redis database.

        Returns:
            Dict[str, Any]: The retrieved state data.
        """
        keys = self.redis_adapter.keys("*")
        state = {}
        for key in keys:
            state[key.decode("utf-8")] = json.loads(self.redis_adapter.get(key))
        return state


class State:
    """
    Class to manage application state using a storage mechanism defined by BaseStorage.
    """

    def __init__(self, storage: BaseStorage) -> None:
        """
        Initializes the State with a specified storage mechanism.

        Args:
            storage (BaseStorage): The storage mechanism for saving and retrieving state.
        """
        self.storage = storage
        self.local_state = self.storage.retrieve_state()

    def set_state(self, key: str, value: Any) -> None:
        """
        Sets a specific state value.

        Args:
            key (str): The key for the state data.
            value (Any): The value to be saved in the state.
        """
        self.local_state[key] = value
        self.storage.save_state(self.local_state)

    def get_state(self, key: str) -> Any:
        """
        Retrieves a specific state value.

        Args:
            key (str): The key for the state data to retrieve.

        Returns:
            Any: The retrieved state value.
        """
        return self.local_state.get(key, None)
