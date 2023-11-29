import os
import logging
import abc
import json
from typing import Any, Dict
from redis import Redis

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SimpleStateManager:
    def __init__(self, person_file_path, genre_file_path, film_work_file_path):
        self.person_file_path = person_file_path
        self.genre_file_path = genre_file_path
        self.film_work_file_path = film_work_file_path

    def set_last_modified(self, key, value):
        file_path = self._get_file_path(key)
        logger.debug(f"Updating '{key}' last modified state to: {value}")
        with open(file_path, 'w') as file:
            file.write(value)

    def get_last_modified(self, key):
        file_path = self._get_file_path(key)
        if not os.path.exists(file_path):
            return None

        with open(file_path, 'r') as file:
            return file.read()

    def _get_file_path(self, key):
        if key == "person":
            return self.person_file_path
        elif key == "genre":
            return self.genre_file_path
        elif key == "film_work":
            return self.film_work_file_path
        else:
            raise ValueError(f"Unknown key: {key}")


class BaseStorage(abc.ABC):
    @abc.abstractmethod
    def save_state(self, state: Dict[str, Any]) -> None:
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> Dict[str, Any]:
        pass


class JsonFileStorage(BaseStorage):
    def __init__(self, file_path: str) -> None:
        self.file_path = file_path

    def save_state(self, state: Dict[str, Any]) -> None:
        with open(self.file_path, 'w') as file:
            json.dump(state, file)

    def retrieve_state(self) -> Dict[str, Any]:
        try:
            with open(self.file_path, 'r') as file:
                return json.load(file)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}


class RedisStorage(BaseStorage):
    def __init__(self, redis_adapter: Redis):
        self.redis_adapter = redis_adapter

    def save_state(self, state: Dict[str, Any]) -> None:
        for key, value in state.items():
            self.redis_adapter.set(key, json.dumps(value))

    def retrieve_state(self) -> Dict[str, Any]:
        keys = self.redis_adapter.keys('*')
        state = {}
        for key in keys:
            state[key.decode('utf-8')] = json.loads(self.redis_adapter.get(key))
        return state


class State:
    def __init__(self, storage: BaseStorage) -> None:
        self.storage = storage
        self.local_state = self.storage.retrieve_state()

    def set_state(self, key: str, value: Any) -> None:
        self.local_state[key] = value
        self.storage.save_state(self.local_state)

    def get_state(self, key: str) -> Any:
        return self.local_state.get(key, None)
