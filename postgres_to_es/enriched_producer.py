import logging
from typing import Generator

from config import EnrichedProducerSettings
from postgres_connection import PostgresConnection
from state import JsonFileStorage, State


class EnrichedProducer:
    def __init__(self, settings: EnrichedProducerSettings, db: PostgresConnection):
        self.settings = settings
        self.db = db
        self.state_manager = State(JsonFileStorage(self.settings.state_file_path))
        self.state = self.state_manager.get_state(self.settings.state_field)

    def produce(self) -> Generator[list[str], None, None]:
        n = 0
        while True:
            n += 1
            logging.info(f"Batch {n}")
            query = self.settings.producer_sql
            args = (self.state, self.settings.limit)
            if not self.state:
                query = "\n".join(
                    line for line in query.split("\n") if not line.startswith("WHERE")
                )
                args = (self.settings.limit,)

            data = self.db.fetch(query, args)

            if not data:
                break

            self.state = data[-1][self.settings.state_field]

            yield [item["id"] for item in data]

    def enrich(self, ids: list[str]) -> list[str]:
        if self.settings.enricher_sql:
            data = self.db.fetch(self.settings.enricher_sql, (tuple(ids),))
            return [item["id"] for item in data]
        else:
            return ids

    def save_state(self):
        self.state_manager.set_state(self.settings.state_field, self.state)

    @property
    def name(self):
        return self.settings.name
