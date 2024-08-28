import collections
import struct

from util import timing_decorator, get_string_hash


def get_doc(pos: int, _len: int):
    with open("./index/index.junk") as f:
        f.seek(pos)
        return f.read(_len)

def token_search(token: str):
    first = get_string_hash(token)
    results = []
    with open(f"index/inverted/{first}.ii", "rb") as f:
        while True:
            length_data = f.read(4)
            if not length_data:
                break
            key_length = struct.unpack('I', length_data)[0]
            key = f.read(key_length).decode('utf-8')
            loc = struct.unpack('I', f.read(4))[0]
            _len = struct.unpack('I', f.read(4))[0]
            if key != token:
                continue
            results.append((loc,_len))
    return results

@timing_decorator
def search(s: str):
    tokens = s.split(" ")
    search_locations = []
    for i in tokens:
        if hits := token_search(i):
            search_locations.extend(hits)
    top_counter = collections.Counter(search_locations)
    best = top_counter.most_common(10)
    for location, score in best:
        print(get_doc(location[0], location[1]))


search("4 WITHERS PL, WESTON")