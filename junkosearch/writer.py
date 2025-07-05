import csv
import os
from collections import defaultdict
from typing import Iterable, Type

from junkosearch.constants import EST_POS_BYTES, DEFAULT_MAX_SEG_SIZE
from junkosearch.document import Document, Field
from junkosearch.handlers import Docfile, Positions, Terms, Skip




class SegmentWriter:
    def __init__(self, seg_no: int, max_size = DEFAULT_MAX_SEG_SIZE):
        self.seg_no = seg_no
        self.docfile = Docfile(seg_no, create=True)
        self.positions = Positions(seg_no, create=True)
        self.terms = Terms(seg_no, create=True)
        self.skip = Skip(seg_no, create=True)

        self.working_index = defaultdict(set, {})
        self.pos_count = 0
        self.open = True
        self.max_size = max_size

    def big_enough(self):
        return self.index_mem_size() > self.max_size

    def index_mem_size(self):
        return self.pos_count * EST_POS_BYTES

    def index(self, token: str):
        self.working_index[token].add(self.docfile.tell())
        self.pos_count += 1

    def close(self):
        pass

    def finalise(self):
        last_skip_code = None
        for full_token, positions in sorted(self.working_index.items(), key=lambda x: x[0]):
            field_id, token = full_token.split("::")
            #this_skip_code = f"{field_id}::{token[:self.skip.size]}"
            this_skip_code = self.skip.skip_code_for_token(field_id, token)
            if this_skip_code != last_skip_code:
                self.skip.store(this_skip_code, self.terms.tell())
                last_skip_code = this_skip_code

            self.terms.store(full_token, self.positions.tell())
            self.positions.store(positions)

        self.docfile.close()
        self.terms.close()
        self.positions.close()
        self.skip.close()
        self.open = False

    def store(self, doc: str, tokens: dict[Field, list[str]]):
        for field, tokens in tokens.items():
            for token in tokens:
                self.index(token)
        self.docfile.store(doc)


def generate_indices(docs: Iterable[Document], n=None, seg_size = DEFAULT_MAX_SEG_SIZE):
    os.makedirs("index", exist_ok=True)
    seg_count = 0
    current_seg = SegmentWriter(seg_count, max_size=seg_size)
    for idx, doc in enumerate(docs):
        if n and idx > n:
            break
        if idx % 50000 == 0 and idx > 0:
            print(f"Processing {idx} - [{round(current_seg.index_mem_size() /1024 /1024 )}mb]")
            if current_seg.big_enough():
                print("Closing segment")
                current_seg.finalise()
                seg_count += 1
                current_seg = SegmentWriter(seg_count, max_size=seg_size)

        current_seg.store(doc.doc_vals(), doc.tokens())

    if current_seg.open:
        current_seg.finalise()


def docs_from_csv(csv_path: str, doc_type: Type[Document], delimiter:str = ",", encoding="utf-8"):
    with open(csv_path, encoding=encoding) as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        for idx, row in enumerate(reader):
            data = {}
            for k,v in doc_type._field_map.items():
                data[k] = row[v.source_name]
            yield doc_type(**data)






