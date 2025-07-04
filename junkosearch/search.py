import collections
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List

from handlers import Docfile, Positions, Terms, Skip
from util import timing


class SegmentReader:
    def __init__(self, seg_no: int):
        self.seg_no = seg_no
        self.docfile = Docfile(seg_no)
        self.positions = Positions(seg_no)
        self.terms = Terms(seg_no)
        self.skip = Skip(seg_no)

    #@timing
    def _get_docs(self, positions: List[int]) -> List[str]:
        return [self.docfile.fetch(i) for i in positions]

    def resolve_term(self, term: str):
        skip_offset = self.skip.lookup(term)
        position_offset = self.terms.lookup(term, skip_offset)
        if not position_offset:
            return []
        return self.positions.fetch(position_offset)

    #@timing
    def search(self, terms: List[str]):
        doc_positions = []

        for term in terms:
            doc_positions.extend(self.resolve_term(term))

        collector = collections.Counter(doc_positions)
        top5 = collector.most_common(10)
        return self._get_docs([i[0] for i in top5])


@timing
def threaded_search(seg_no: int, terms: List[str]) -> List[str]:
    @timing
    def worker(term: str) -> List[int]:
        sr = SegmentReader(seg_no)
        return sr.resolve_term(term)

    doc_positions = []
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(worker, term) for term in terms]
        for future in as_completed(futures):
            doc_positions.extend(future.result())

    collector = collections.Counter(doc_positions)
    top5 = collector.most_common(10)

    final_reader = SegmentReader(seg_no)
    return final_reader._get_docs([i[0] for i in top5])

search_query = "RIV 986 WIL CAT FLAT"

results = threaded_search(0, search_query.split(" "))

print("\n".join(results))


# reader = SegmentReader(0)
#
# search_terms = "113 CANBERRA GRIFFITH"
#
# results = reader.search(search_terms.split(" "))
#
# print("\n".join(results))
