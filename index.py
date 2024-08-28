import csv
from typing import Tuple
import struct
import os

from util import get_string_hash


def get_data():
    with open("./misc/test_data.csv") as f:
        reader = csv.DictReader(f)
        for idx, i in enumerate(reader):
            yield {
            "formattedAddress": i["label"],
            "addressId": i["id"]
        }



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


def generate_indices(data):
    handlers = {}
    with open("index/index.junk", "w+") as f:
        ctr = 0
        for i in data:
            if ctr % 10000 == 0:
                print(f"Processing {ctr}")
            label, _id = i.values()
            tokens = label.split(" ")
            serialised_string = f"{label}~{_id}"
            entry_len = len(serialised_string)
            for token in tokens:
                write_inverted_entry(handlers, token, (f.tell(), entry_len))
            f.write(serialised_string)
            ctr += 1
    for f in handlers.values():
        f.close()



generate_indices(get_data())



