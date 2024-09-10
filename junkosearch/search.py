import collections
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

    @timing
    def _get_docs(self, positions: List[int]) -> List[str]:
        return [self.docfile.fetch(i) for i in positions]

    @timing
    def search(self, terms: List[str]):
        doc_positions = []
        for term in terms:
            position_offset = self.terms.lookup(term, self.skip.lookup(term[:2]))
            if not position_offset:
                continue
            doc_positions.extend(self.positions.fetch(position_offset))
        collector = collections.Counter(doc_positions)
        top5 = collector.most_common(5)

        return self._get_docs([i[0] for i in top5])


reader = SegmentReader(0)

search_terms = "1 SMITH R"

results = reader.search([search_terms])

print("\n".join(results))
