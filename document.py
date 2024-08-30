from abc import ABC, abstractmethod
from typing import Tuple, Iterable, Optional, Type


class Tokeniser(ABC):

    @abstractmethod
    def __init__(self):
        ...
    @abstractmethod
    def tokenise(self, s: str) -> Iterable[str]:
        ...

class SimpleTokeniser(Tokeniser):
    def __init__(self):
        return
    def tokenise(self, s: str) -> Iterable[str]:
        return s.split(" ")


class Field:
    def __init__(self, name: str, store: bool, index: bool, tokeniser: Type[Tokeniser] = SimpleTokeniser):
        self.name = name
        self.store = store
        self.index = index
        self.tokeniser = tokeniser() if index else None


class Document:
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            if key in self._field_map:
                setattr(self, key, value)
            else:
                raise ValueError(f"Unknown field: {key}")

    @classmethod
    def _create_field_map(cls):
        cls._field_map = {}
        for attr_name, attr_value in cls.__annotations__.items():
            field = getattr(cls, attr_name)
            if isinstance(field, Field):
                cls._field_map[attr_name] = field

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls._create_field_map()

class GnafDocument(Document):
    id: str = Field(name="ADDRESS_DETAIL_PID", store=True, index=False)
    label: str = Field(name="ADDRESS_LABEL", store=True, index=True)


foo = GnafDocument(id="123", label="banana")

print()