from dataclasses import dataclass, field
from typing import Optional


@dataclass
class PersonItem:
    id: str
    name: str


@dataclass
class ESItem:
    id: str
    title: str
    imdb_rating: float = None
    genre: str = None
    description: str = None
    director: str = None
    actors_names: str = None
    writers_names: str = None
    actors: list[PersonItem] = field(default_factory=list)
    writers: list[PersonItem] = field(default_factory=list)
