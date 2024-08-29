import collections
import struct
from typing import Optional, List, Tuple

from util import get_string_hash, timing


class SegmentReader:
    def __init__(self, seg_no: int):
        self.docfile_handler = open(f"index/docfile_{seg_no}.junk", "rb")
        self.position_handler = open(f"index/positions_{seg_no}.pos", "rb")
        self.terms_handler = open(f"index/terms_{seg_no}.tii", "rb")
        self.skip_handler = open(f"index/terms_{seg_no}.tsk", "rb")


    def _get_skip(self, term: str) -> Optional[int]:
        self.skip_handler.seek(0, 0)


        while True:
            length_data = self.skip_handler.read(4)
            if not length_data:
                break
            key_length = struct.unpack('I', length_data)[0]
            key = self.skip_handler.read(key_length).decode('utf-8')
            loc = struct.unpack('I', self.skip_handler.read(4))[0]
            if key == term:
                return loc
            if key > term:
                break
        return None

    @timing
    def _get_position_offset(self, term: str, start_offset: int) -> Optional[int]:
        self.terms_handler.seek(start_offset,0)
        while True:
            length_data = self.terms_handler.read(4)
            if not length_data:
                break
            key_length = struct.unpack('I', length_data)[0]
            key = self.terms_handler.read(key_length).decode('utf-8')
            loc = struct.unpack('I', self.terms_handler.read(4))[0]
            if key == term:
                return loc
            if key > term:
                break
        return None

    @timing
    def _get_docfile_offset(self, position_offset: int) -> List[int]:
        self.position_handler.seek(position_offset,0)
        positions_len = struct.unpack("I", self.position_handler.read(4))[0]
        positions_data = self.position_handler.read(4 * positions_len)
        positions = list(struct.unpack(f"{positions_len}I", positions_data))
        return positions

    @timing
    def _get_docs(self, positions: List[int]) -> List[str]:
        docs = []
        for offset in positions:
            self.docfile_handler.seek(offset,0)
            doc_len = struct.unpack("I", self.docfile_handler.read(4))[0]

            docs.append(self.docfile_handler.read(doc_len).decode("utf-8"))
        return docs

    @timing
    def search(self, terms: List[str]):
        doc_positions = []
        for term in terms:
            position_offset = self._get_position_offset(term, self._get_skip(term[:2]))
            if not position_offset:
                continue
            doc_positions.extend(self._get_docfile_offset(position_offset))
        collector = collections.Counter(doc_positions)
        top5 = collector.most_common(5)

        return self._get_docs([i[0] for i in top5])




reader = SegmentReader(0)

search_terms = "13 MCDOWALL PL, KAMBAH"

results = reader.search(search_terms.split(" "))

print("\n".join(results))
#
# def get_doc(pos: int, _len: int):
#     with open("./index/index.junk") as f:
#         f.seek(pos)
#         return f.read(_len)
#
# def token_search(token: str):
#     first = get_string_hash(token)
#     results = []
#     with open(f"index/inverted/{first}.ii", "rb") as f:
#         while True:
#             length_data = f.read(4)
#             if not length_data:
#                 break
#             key_length = struct.unpack('I', length_data)[0]
#             key = f.read(key_length).decode('utf-8')
#             loc = struct.unpack('I', f.read(4))[0]
#             _len = struct.unpack('I', f.read(4))[0]
#             if key != token:
#                 continue
#             results.append((loc,_len))
#     return results
#
# @timing_decorator
# def search(s: str):
#     tokens = s.split(" ")
#     search_locations = []
#     for i in tokens:
#         if hits := token_search(i):
#             search_locations.extend(hits)
#     top_counter = collections.Counter(search_locations)
#     best = top_counter.most_common(10)
#     for location, score in best:
#         print(get_doc(location[0], location[1]))


#search("4 WITHERS PL, WESTON")