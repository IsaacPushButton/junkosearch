import gc
import hashlib
import time
from functools import wraps
import sys

def timing(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        # Record the start time
        start_time = time.time()
        # Call the original function
        result = func(*args, **kwargs)

        # Record the end time
        end_time = time.time()

        # Calculate the duration
        duration = end_time - start_time

        # Print or log the duration
        print(f"Function {args[0].__class__.__name__}.{func.__name__} took {duration:.4f} seconds to execute.")

        return result

    return wrapper


def get_string_hash(input_string):
    """Compute the SHA-256 hash of a given string."""
    # Create a SHA-256 hash object
    sha256 = hashlib.sha256()

    # Update the hash object with the bytes of the input string
    sha256.update(input_string.encode('utf-8'))

    # Return the hexadecimal representation of the digest
    return sha256.hexdigest()[:3]

def get_obj_size(obj):
    marked = {id(obj)}
    obj_q = [obj]
    sz = 0

    while obj_q:
        sz += sum(map(sys.getsizeof, obj_q))

        # Lookup all the object referred to by the object in obj_q.
        # See: https://docs.python.org/3.7/library/gc.html#gc.get_referents
        all_refr = ((id(o), o) for o in gc.get_referents(*obj_q))

        # Filter object that are already marked.
        # Using dict notation will prevent repeated objects.
        new_refr = {o_id: o for o_id, o in all_refr if o_id not in marked and not isinstance(o, type)}

        # The new obj_q will be the ones that were not marked,
        # and we will update marked with their ids so we will
        # not traverse them again.
        obj_q = new_refr.values()
        marked.update(new_refr.keys())

    return sz