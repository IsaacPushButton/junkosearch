import struct
from typing import Optional, List, Tuple


ROOT_PATH = ""

class Docfile:
    """
    Big bucket of data where we store the actual data
    """
    def __init__(self, seg_no: int, create=False):
        mode = "w" if create else "r"
        self.handler = open(f"{ROOT_PATH}index/docfile_{seg_no}.junk", f"{mode}b+")

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
        self.handler = open(f"{ROOT_PATH}index/terms_{seg_no}.tsk", f"{mode}b+")

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
        self.handler.seek(0,2)
        marker = self.handler.tell()
        self.handler.write(struct.pack('I', len(key)))
        self.handler.write(key.encode("utf-8"))
        self.handler.write(struct.pack("I", term_marker))
        return marker

    def lookup(self, term: str) -> Optional[int]:
        """
        :param term: A full term we are looking for, does not need to exist in the skip file
        :return: An offset in the terms file to start looking
        """
        self.handler.seek(0, 0)
        while True:
            length_data = self.handler.read(4)
            if not length_data:
                break
            key_length = struct.unpack('I', length_data)[0]
            key = self.handler.read(key_length).decode('utf-8')
            loc = struct.unpack('I', self.handler.read(4))[0]
            if key < term:
                continue
            return loc
        return None


class Terms:
    """
    A file containing the list of terms in the inverted index, along with the offset to find
    matching docs in the positions file
    """
    def __init__(self, seg_no: int, create=False):
        mode = "w" if create else "r"
        self.handler = open(f"{ROOT_PATH}index/terms_{seg_no}.tii", f"{mode}b+")

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


class Positions:
    def __init__(self, seg_no: int, create=False):
        mode = "w" if create else "r"
        self.handler = open(f"{ROOT_PATH}index/positions_{seg_no}.pos", f"{mode}b+")

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
        for offset in offsets:
            self.handler.write(struct.pack('I', offset))
        return marker

    def fetch(self, offset: int) -> Tuple[int]:
        """
        :param offset:
        :return:  A list of docfile offsets sharing a term
        """
        self.handler.seek(offset, 0)
        positions_len = struct.unpack("I", self.handler.read(4))[0]
        positions_data = self.handler.read(4 * positions_len)
        positions = struct.unpack(f"{positions_len}I", positions_data)
        return positions  # noqa its an int tuple okay
