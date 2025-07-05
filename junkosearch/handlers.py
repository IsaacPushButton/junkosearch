import os
import struct
from mmap import mmap
from typing import Optional, List, Tuple

# from junkosearch.util import timing

ROOT_PATH = "./"

class Docfile:
    """
    Big bucket of data where we store the actual data
    """
    def __init__(self, seg_no: int, create=False):
        mode = "w" if create else "r"
        self.handler = open(f"{ROOT_PATH}/index/docfile_{seg_no}.junk", f"{mode}b")

    def tell(self):
        return self.handler.tell()

    def close(self):
        self.handler.close()

    def store(self, doc: str):
        """
        :param doc: the document we want to store
        :return: Offset we stored the doc at
        """
        self.handler.seek(0,2)
        marker = self.handler.tell()
        self.handler.write(struct.pack('I', len(doc)))
        self.handler.write(doc.encode("utf-8"))
        return marker

    #@timing
    def fetch(self, idx: int) -> str:
        """
        :param idx: The offset to find the document
        :return: The document
        """
        self.handler.seek(idx, 0)
        doc_len = struct.unpack("I", self.handler.read(4))[0]
        return self.handler.read(doc_len).decode("utf-8")


class Skip:
    """
    'Index' file where we store a sparse list of keys and their location in the terms file so we can find a good starting
    point for looking in the terms file
    """
    def __init__(self, seg_no: int, create=False):
        mode = "w" if create else "r"
        self.handler = open(f"{ROOT_PATH}index/terms_{seg_no}.tsk", f"{mode}b")
        self.size = 4
        self.file_size = os.fstat(self.handler.fileno()).st_size


    def tell(self):
        return self.handler.tell()

    def close(self):
        self.handler.close()

    def store(self, key: str, term_marker: int):
        """
        :param key: Key to store
        :param term_marker: An offset in the terms file
        :return: Offset we stored the skip at
        """
        key = key[:self.size].ljust(self.size)
        self.handler.seek(0,2)
        marker = self.handler.tell()
        self.handler.write(key.encode("utf-8"))
        self.handler.write(struct.pack("I", term_marker))
        return marker

    #@timing
    def lookup(self, term: str) -> Optional[int]:
        """
        :param term: A full term we are looking for, does not need to exist in the skip file
        :return: An offset in the terms file to start looking
        """
        term = term[:self.size].ljust(self.size)

        record_size = self.size + 4 # <size> string + 4 byte int

        # binary search from chatgpt
        num_records = self.file_size // record_size

        lo = 0
        hi = num_records - 1
        result = None

        while lo <= hi:
            mid = (lo + hi) // 2
            offset = mid * record_size
            self.handler.seek(offset)

            key = self.handler.read(4).decode("utf-8")
            loc = struct.unpack("I", self.handler.read(4))[0]

            if key < term:
                lo = mid + 1
            else:
                result = loc
                hi = mid - 1

        return result


class Terms:
    """
    A file containing the list of terms in the inverted index, along with the offset to find
    matching docs in the positions file
    """
    def __init__(self, seg_no: int, create=False):
        mode = "w" if create else "r"
        self.handler = open(f"{ROOT_PATH}index/terms_{seg_no}.tii", f"{mode}b")

    def tell(self):
        return self.handler.tell()

    def close(self):
        self.handler.close()

    def store(self, term: str, positions_marker: int):
        """
        :param term: The term to store in the inverted index
        :param positions_marker: Offset that related doc locations are stored in the positions file
        :return: The offset we stored the term at
        """
        self.handler.seek(0, 2)
        marker = self.handler.tell()
        key_encoded = term.encode('utf-8')
        self.handler.write(struct.pack('I', len(key_encoded)))
        self.handler.write(key_encoded)  # Key
        self.handler.write(struct.pack("I", positions_marker))
        return marker
    #@timing
    def lookup(self, term: str,  start_at: int) -> Optional[int]:
        """
        :param term: A term to find in the inverted index
        :param start_at: Offset byte to start looking
        :return: An offset to find the doc locations in the positions file
        """
        self.handler.seek(start_at, 0)
        while True:
            length_data = self.handler.read(4)
            if not length_data:
                break
            key_length = struct.unpack('I', length_data)[0]
            key = self.handler.read(key_length).decode('utf-8')
            loc = struct.unpack('I', self.handler.read(4))[0]
            if key == term:
                return loc
            if key > term:
                break
        return None


def encode_varint(value: int) -> bytes:
    result = []
    while value > 0x7F:
        result.append((value & 0x7F) | 0x80)
        value >>= 7
    result.append(value & 0x7F)
    return bytes(result)

def decode_varint(data: bytes, start: int) -> Tuple[int, int]:
    shift = 0
    result = 0
    idx = start
    while True:
        byte = data[idx]
        idx += 1
        result |= (byte & 0x7F) << shift
        if byte & 0x80 == 0:
            break
        shift += 7
    return result, idx

class Positions:
    def __init__(self, seg_no: int, create=False):
        mode = "w" if create else "r"

        #self.file = open(f"{ROOT_PATH}index/positions_{seg_no}.pos", f"{mode}b+")
        # if create:
        #     self.handler = self.file
        # else:
        #     self.handler = mmap(self.file.fileno(), 0)
        self.handler = open(f"{ROOT_PATH}index/positions_{seg_no}.pos", f"{mode}b")

    def tell(self):
        return self.handler.tell()

    def close(self):
        self.handler.close()

    def store(self, offsets: List[int]) -> int:
        """
        :param offsets: List of offsets in the docfile that represent documents sharing a term
        :return: The offset we stored the positions at
        """
        self.handler.seek(0, 2)
        marker = self.handler.tell()
        self.handler.write(struct.pack("I", len(offsets)))
        prev_offset = 0
        for offset in sorted(offsets):
            delta = offset - prev_offset
            encoded_delta = encode_varint(delta)
            self.handler.write(encoded_delta)
            prev_offset = offset
        return marker

    #@timing
    def fetch(self, offset: int) -> Tuple[int]:
        """
        :param offset:
        :return:  A list of docfile offsets sharing a term
        """
        self.handler.seek(offset, 0)
        positions_len = struct.unpack("I", self.handler.read(4))[0]
        positions_data = self.handler.read(4 * positions_len)

        idx = 0
        prev_offset = 0
        positions = []
        for _ in range(positions_len):
            delta, idx = decode_varint(positions_data, idx)
            doc_id = prev_offset + delta
            positions.append(doc_id)
            prev_offset = doc_id

        return tuple(positions) # noqa

