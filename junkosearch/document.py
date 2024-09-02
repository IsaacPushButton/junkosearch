from typing import List, Optional

from junkosearch.tokeniser import Tokeniser


class Field:
    def __init__(self, name: str, store: bool, index: bool, tokenisers: Optional[List[Tokeniser]] = None):
        self.name = name
        self.store = store
        self.index = index
        self.tokenisers = tokenisers


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
        tokens = []
        for k,v in self._field_map.items():
            if v.index:
                for tokeniser in v.tokenisers:
                    tokens.extend(tokeniser.tokenise(self.__getattribute__(k)))
        return tokens
    def doc_vals(self):
        vals = []
        for k,v in self._field_map.items():
            if v.store:
                vals.append(self.__getattribute__(k))
        return "~".join(vals)

