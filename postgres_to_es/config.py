from pydantic import BaseModel


class DSNSettings(BaseModel):
    dbname: str
    user: str
    host: str
    port: int
    password: str = None


class ESSettings(BaseModel):
    host: str
    index: str


class EnrichedProducerSettings(BaseModel):
    name: str
    state_file_path: str
    limit: int
    state_field: str
    producer_sql: str
    enricher_sql: str = None


class Config(BaseModel):
    dsn: DSNSettings
    es: ESSettings
    enriched_producers: list[EnrichedProducerSettings]
    merger_sql: str
