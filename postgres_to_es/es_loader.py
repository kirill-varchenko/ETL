import json
from dataclasses import asdict

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from backoff import backoff
from es_item import ESItem


class ESLoader:
    def __init__(self, host: str, index: str):
        self.host = host
        self.index = index
        self.es = None
        self.es_init()

    @backoff()
    def es_init(self):
        self.es = Elasticsearch([self.host])

    def make_body(self, data: list[ESItem]) -> list[dict]:
        res = []
        for item in data:
            res.append({"index": {"_index": self.index, "_id": item.id}})
            res.append(asdict(item))
        return res

    @backoff()
    def load(self, data: list[ESItem]):
        body = self.make_body(data)
        res = self.es.bulk(body=body)
