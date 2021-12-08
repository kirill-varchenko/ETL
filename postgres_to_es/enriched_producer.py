import logging
from typing import Generator

from config import EnrichedProducerSettings
from postgres_connection import PostgresConnection
from state import JsonFileStorage, State


class EnrichedProducer:
    def __init__(self, settings: EnrichedProducerSettings, db: PostgresConnection):
        self.settings = settings
        self.db = db
        self.state = State(JsonFileStorage(self.settings.state_file_path))

    def produce(self) -> Generator[list[str], None, None]:
        n = 0
        while True:
            n += 1
            logging.info(f"Batch {n}")
            last_state = self.state.get_state(self.settings.state_field)
            query = self.settings.producer_sql
            args = (last_state, self.settings.limit)
            if not last_state:
                query = "\n".join(
                    line for line in query.split("\n") if not line.startswith("WHERE")
                )
                args = (self.settings.limit,)

            data = self.db.fetch(query, args)

            if not data:
                break

            new_state = data[-1][self.settings.state_field]
            self.state.set_state(self.settings.state_field, new_state)
            yield [item["id"] for item in data]

    def enrich(self, ids: list[str]) -> list[str]:
        if self.settings.enricher_sql:
            data = self.db.fetch(self.settings.enricher_sql, (tuple(ids),))
            return [item["id"] for item in data]
        else:
            return ids

    @property
    def name(self):
        return self.settings.name
