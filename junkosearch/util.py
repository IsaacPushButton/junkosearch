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
        print(f"Function {args[0].__class__.__name__}.{func.__name__} {str(args)[:50]}{'...' if len(str(args)) > 50 else ''} took {duration:.4f} seconds to execute.")

        return result

    return wrapper
