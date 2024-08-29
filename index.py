import csv
from collections import defaultdict
from typing import Tuple
import struct
import os
import sys
from util import get_string_hash, get_obj_size

MAX_SEG_SIZE = 1000 * 1024 * 1024

EST_POS_BYTES = 41

def get_data():
    with open("./misc/GNAF_CORE.psv") as f:
        reader = csv.DictReader(f, delimiter="|")
        for idx, i in enumerate(reader):
            yield {
            "formattedAddress": i['ADDRESS_LABEL'],
            "addressId": i["\ufeffADDRESS_DETAIL_PID"]
        }


#
def write_inverted_entry(handlers, key: str, val: Tuple[int,int]):
    fkey = get_string_hash(key)
    if not os.path.isdir("index/inverted"):
        os.mkdir("index/inverted")
    if not handlers.get(fkey):
        handlers[fkey] = open(f"index/inverted/{fkey}.ii", "wb+")
    f = handlers.get(fkey)
    key_encoded = key.encode('utf-8')
    f.write(struct.pack('I', len(key_encoded)))  # Length of the key
    f.write(key_encoded)  # Key
    f.write(struct.pack('I', val[0]))  # Value
    f.write(struct.pack('I', val[1]))  # Value

class SegmentWriter:
    def __init__(self, seg_num: int):
        self.seg_num = seg_num
        self.docfile_handler = open(f"index/docfile_{seg_num}.junk", "wb+")
        self.terms_index =  open(f"index/terms_{seg_num}.tii", "wb+")
        self.positions_handler = open(f"index/positions_{seg_num}.pos", "wb+")
        self.terms_skip = open(f"index/terms_{seg_num}.tsk", "wb+")
        self.inverted_index = defaultdict(list, {})
        self.pos_count = 0

    def should_close(self):
        return self.index_mem_size() > MAX_SEG_SIZE

    def index_mem_size(self):
        return self.pos_count * EST_POS_BYTES

    def index(self, token: str):
        self.inverted_index[token].append(self.docfile_handler.tell())
        self.pos_count += 1

    def store(self,  doc: str):
        self.docfile_handler.write(struct.pack('I', len(doc)))
        self.docfile_handler.write(doc.encode("utf-8"))

    def close(self):
        last_skip_code = None
        for key, positions in sorted(self.inverted_index.items(), key=lambda x: x[0]):
            this_skip_code = key[:2]
            if this_skip_code != last_skip_code:
                self.terms_skip.write(struct.pack('I', len(this_skip_code)))
                self.terms_skip.write(this_skip_code.encode("utf-8"))
                self.terms_skip.write(struct.pack("I", self.terms_index.tell()))
                last_skip_code = this_skip_code

            key_encoded = key.encode('utf-8')
            self.terms_index.write(struct.pack('I', len(key_encoded)))
            self.terms_index.write(key_encoded)  # Key
            self.terms_index.write(struct.pack("I", self.positions_handler.tell()))

            self.positions_handler.write(struct.pack("I", len(positions)))
            for offset in positions:
                self.positions_handler.write(struct.pack('I', offset))

        self.docfile_handler.close()
        self.terms_index.close()
        self.positions_handler.close()

def generate_indices(data):
    seg_count = 0
    current_seg = SegmentWriter(seg_count)
    for idx, i in enumerate(data):
        if idx % 50000 == 0 and idx > 0:
            print(f"Processing {idx} - [{round(current_seg.index_mem_size() /1024 /1024 )}mb]")
            if current_seg.should_close():
                print("Closing segment")
                current_seg.close()
                break
        label, _id = i.values()
        tokens = label.split(" ")
        serialised_string = f"{label}~{_id}"
        for token in tokens:
            current_seg.index(token)
        current_seg.store(serialised_string)




generate_indices(get_data())



