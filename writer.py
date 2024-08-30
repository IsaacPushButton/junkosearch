import csv
from collections import defaultdict

from handlers import Docfile, Skip, Terms, Positions

MAX_SEG_SIZE = 5 * 1024 * 1024

EST_POS_BYTES = 41


class SegmentWriter:
    def __init__(self, seg_no: int):
        self.seg_no = seg_no
        self.docfile = Docfile(seg_no, create=True)
        self.positions = Positions(seg_no, create=True)
        self.terms = Terms(seg_no, create=True)
        self.skip = Skip(seg_no, create=True)

        self.working_index = defaultdict(list, {})
        self.pos_count = 0
        self.open = True

    def big_enough(self):
        return self.index_mem_size() > MAX_SEG_SIZE

    def index_mem_size(self):
        return self.pos_count * EST_POS_BYTES

    def index(self, token: str):
        self.working_index[token].append(self.docfile.tell())
        self.pos_count += 1

    def close(self):
        pass

    def finalise(self):
        last_skip_code = None
        for key, positions in sorted(self.working_index.items(), key=lambda x: x[0]):
            this_skip_code = key[:2]
            if this_skip_code != last_skip_code:
                self.skip.store(this_skip_code, self.terms.tell())

            self.terms.store(key, self.positions.tell())
            self.positions.store(positions)

        self.docfile.close()
        self.terms.close()
        self.positions.close()
        self.skip.close()
        self.open = False

    def store(self, doc: str, *tokens: str):
        for token in tokens:
            self.index(token)
        self.docfile.store(doc)


def generate_indices(data):
    seg_count = 0
    current_seg = SegmentWriter(seg_count)
    for idx, i in enumerate(data):
        if idx % 50000 == 0 and idx > 0:
            print(f"Processing {idx} - [{round(current_seg.index_mem_size() /1024 /1024 )}mb]")
            if current_seg.big_enough():
                print("Closing segment")
                current_seg.finalise()
                break
        label, _id = i.values()
        tokens = label.split(" ")
        serialised_string = f"{label}~{_id}"
        current_seg.store(serialised_string, *tokens)

    if current_seg.open:
        current_seg.finalise()





def get_data():
    with open("./misc/GNAF_CORE.psv") as f:
        reader = csv.DictReader(f, delimiter="|")
        for idx, i in enumerate(reader):
            yield {
            "formattedAddress": i['ADDRESS_LABEL'],
            "addressId": i["\ufeffADDRESS_DETAIL_PID"]
        }


generate_indices(get_data())



