from abc import ABC, abstractmethod
from typing import Iterable, Optional


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


class EdgeNgram(Tokeniser):
    def __init__(self, min_len: int, max_len: int, split: Optional[str] = None):
        self.min_len = min_len
        self.max_len = max_len
        self.split = split

    def tokenise(self, s: str) -> Iterable[str]:
        to_tokenise = s.split(self.split) if self.split else [s]
        tokens = []
        for v in to_tokenise:
            for _len in range(self.min_len, min(self.max_len, len(v))):
                tokens.append(v[:_len])
        return tokens
