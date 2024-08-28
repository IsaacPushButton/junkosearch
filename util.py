import hashlib
import time
from functools import wraps


def timing_decorator(func):
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
        print(f"Function {func.__name__} took {duration:.4f} seconds to execute.")

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
