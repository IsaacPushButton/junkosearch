import cProfile
import collections
import glob
import heapq
import pstats
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
from handlers import Docfile, Positions, Terms, Skip
from collections import defaultdict

from junkosearch.constants import FIELD_ID_PREFIX_LEN
from junkosearch.document import Field
from junkosearch.util import timing
from load_gnaf import GnafDocument

class SegmentReader:
    def __init__(self, seg_no: int):
        self.seg_no = seg_no
        self.docfile = Docfile(seg_no)
        self.positions = Positions(seg_no)
        self.terms = Terms(seg_no)
        self.skip = Skip(seg_no)

    #@timing
    def _get_docs(self, positions: list[int]) -> list[str]:
        return [self.docfile.fetch(i) for i in positions]

    #@timing
    def resolve_term(self, term: str):
        field_id, token = term.split("::")
        skip_offset = self.skip.lookup(self.skip.skip_code_for_token(field_id, token))
        if not skip_offset:
            return []
        position_offset = self.terms.lookup(term, skip_offset)
        if not position_offset:
            return []
        return self.positions.fetch(position_offset)

    #@timing
    def search(self, terms: list[str]):
        doc_positions = []

        for term in terms:
            doc_positions.extend(self.resolve_term(term))

        collector = collections.Counter(doc_positions)
        top5 = collector.most_common(10)
        return self._get_docs([i[0] for i in top5])



def collect_top_n(segment_hits: dict[str, list[int]], max_score: int,  n: int = 5, early_exit:bool = False):
    freq = defaultdict(int)

    topN_early = []

    for seg, positions in segment_hits.items():
        for pos in positions:
            freq[(seg, pos)] += 1
            if early_exit and freq[(seg, pos)] == max_score:
                topN_early.append((freq[(seg, pos)], (seg, pos)))
                if len(topN_early) == n:
                    break
        if len(topN_early) == n:
            break

    if len(topN_early) < n:
        items = [(count, key) for key, count in freq.items()]
        topN = heapq.nlargest(n, items)
    else:
        topN = topN_early

    to_fetch = defaultdict(list)
    for _, (seg, pos)in topN:
        to_fetch[seg].append(pos)
    return to_fetch

def fetch_results(to_fetch):
    final_results = []
    for seg, positions in to_fetch.items():
        reader = SegmentReader(seg)
        final_results.extend(reader._get_docs(positions))
    return final_results

@lru_cache
def get_reader(seg: int) -> SegmentReader:
    return SegmentReader(seg)

@timing
def threaded_search(terms: list[str]) -> list[str]:
    seg_count = len(glob.glob("./index/**.junk"))

    def worker(seg_reader: SegmentReader, terms: list[str], seg_no: int):
        hits = []
        for term in terms:
            hits.extend(seg_reader.resolve_term(term))
        return hits, seg_no

    segment_hits = defaultdict(list)
    with ThreadPoolExecutor() as executor:
        futures = []
        for seg in range(seg_count):
            sr = get_reader(seg)
            futures.append(executor.submit(worker,sr, terms, seg))

        results = [future.result() for future in futures]
    for hits, seg in results:
        segment_hits[seg].extend(hits)

    to_fetch = collect_top_n(segment_hits, max_score=len(terms))

    final_results = fetch_results(to_fetch)

    return final_results

def q(field, term: str):
    terms = field.tokenisers[0].search_tokenise(term)

    return [f"{field.query_code[:FIELD_ID_PREFIX_LEN]}::{i}" for i in terms]

WARM_SEGMENTS = [get_reader(i) for i in  range(len(glob.glob("./index/**.junk")))]

search_query = [
    *q(GnafDocument.street_number_1, "113"),
    *q(GnafDocument.street_name, "CANBERRA"),
    *q(GnafDocument.locality_name, "GRIFFITH"),
]
results = threaded_search(search_query)
print("\n".join(results))


