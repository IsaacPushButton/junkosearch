from collections import defaultdict

from junkosearch.constants import FIELD_ID_PREFIX_LEN
from junkosearch.tokeniser import Tokeniser


class Field:
    def __init__(self, source_name: str, store: bool, index: bool, tokenisers: list[Tokeniser] | None = None, query_code: str = None):
        self.source_name = source_name
        self.store = store
        self.index = index
        self.tokenisers = tokenisers
        self.query_code = query_code


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

    def tokens(self):
        tokens = defaultdict(list)
        for k,v in self._field_map.items():
            if v.index:
                for tokeniser in v.tokenisers:
                    tokens[v].extend([f"{v.query_code[:FIELD_ID_PREFIX_LEN]}::{i}" for i in tokeniser.tokenise(self.__getattribute__(k))])
        return tokens
    def doc_vals(self):
        vals = []
        for k,v in self._field_map.items():
            if v.store:
                vals.append(self.__getattribute__(k))
        return "~".join(vals)

