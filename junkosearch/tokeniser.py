from abc import ABC, abstractmethod
from typing import Iterable, Optional


class Tokeniser(ABC):

    @abstractmethod
    def __init__(self):
        ...
    @abstractmethod
    def tokenise(self, s: str) -> Iterable[str]:
        ...
    @abstractmethod
    def search_tokenise(self, s: str) -> Iterable[str]:
        ...


class NothingTokeniser(Tokeniser):
    def __init__(self):
        return
    def tokenise(self, s: str) -> Iterable[str]:
        return [s]
    def search_tokenise(self, s: str) -> Iterable[str]:
        return self.tokenise(s)

class SimpleTokeniser(Tokeniser):
    def __init__(self):
        return

    def tokenise(self, s: str) -> Iterable[str]:
        return s.split(" ")

    def search_tokenise(self, s: str) -> Iterable[str]:
        return self.tokenise(s)


class EdgeNgram(Tokeniser):
    def __init__(self, min_len: int, max_len: int, split: Optional[str] = None):
        self.min_len = min_len
        self.max_len = max_len
        self.split = split

    def tokenise(self, s: str) -> Iterable[str]:
        to_tokenise = s.split(self.split) if self.split else [s]
        tokens = []
        for v in to_tokenise:
            if len(v) < self.min_len:
                tokens.append(v)
                continue
            for _len in range(self.min_len, min(self.max_len, len(v) + 1)):
                tokens.append(v[:_len])
        return tokens

    def search_tokenise(self, s: str) -> Iterable[str]:
        tokens = s.split(self.split) if self.split else [s]
        return [i[:self.max_len - 1] for i in tokens]



