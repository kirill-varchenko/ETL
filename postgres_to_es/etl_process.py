import logging
import itertools

from config import Config, DSNSettings, EnrichedProducerSettings, ESSettings
from enriched_producer import EnrichedProducer
from es_item import ESItem, PersonItem
from es_loader import ESLoader
from postgres_connection import PostgresConnection


def grouper_it(iterable, n):
    it = iter(iterable)
    while True:
        chunk_it = itertools.islice(it, n)
        try:
            first_el = next(chunk_it)
        except StopIteration:
            return
        yield itertools.chain((first_el,), chunk_it)


class ETLProcess:
    def __init__(self, config: Config):
        self.db = PostgresConnection(config.dsn)
        self.enriched_producers = [
            EnrichedProducer(ep, self.db) for ep in config.enriched_producers
        ]
        self.merger_sql = config.merger_sql
        self.es_limit = config.es_limit
        self.es_loader = ESLoader(config.es.host, config.es.index)

    def merge(self, ids: list[str]) -> list:
        data = self.db.fetch(self.merger_sql, (tuple(ids),))
        return data

    def transform(self, data: list[dict]) -> list[ESItem]:
        key_func = lambda x: x["fw_id"]
        res = []
        data.sort(key=key_func)
        for fw_id, fw_group_iter in groupby(data, key=key_func):
            fw_group = list(fw_group_iter)

            item = ESItem(
                id=fw_id,
                imdb_rating=fw_group[0]["rating"],
                title=fw_group[0]["title"],
                description=fw_group[0]["description"],
            )

            directors = set()
            genres = set()
            actors = {}
            writers = {}

            for elem in fw_group:
                if elem["name"]:
                    genres.add(elem["name"])
                if elem["role"] == "director":
                    directors.add(elem["full_name"])
                if elem["role"] == "actor":
                    actors[elem["id"]] = elem["full_name"]
                if elem["role"] == "writer":
                    writers[elem["id"]] = elem["full_name"]

            item.genre = " ".join(genres) if genres else None
            item.director = " ".join(directors) if directors else None
            if actors:
                item.actors_names = list(actors.values())
                item.actors = [
                    PersonItem(id=person_id, name=person_name)
                    for person_id, person_name in actors.items()
                ]
            if writers:
                item.writers_names = list(writers.values())
                item.writers = [
                    PersonItem(id=person_id, name=person_name)
                    for person_id, person_name in writers.items()
                ]

            res.append(item)

        return res

    def process(self):
        ids_to_update = set()
        for enriched_producer in self.enriched_producers:
            logging.info(f"Running {enriched_producer.name}")
            for batch in enriched_producer.produce():
                ids = enriched_producer.enrich(batch)
                ids_to_update |= set(ids)

        for ids in grouper_it(ids_to_update, self.es_limit):
            postgres_data = self.merge(ids)
            transformed_data = self.transform(postgres_data)
            self.es_loader.load(transformed_data)

        for enriched_producer in self.enriched_producers:
            enriched_producer.save_state()

