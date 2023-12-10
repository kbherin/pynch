from functools import reduce
from typing import Callable
import pandas as pd
import time

def coalesce(*args):
    return reduce(lambda x,y: x if not pd.isnull(x) else y, args)

def list_remove(l, ser):
    for i,it in enumerate(l):
        if id(it) == id(ser):
            l.pop(i)
            break
    return l

def find_first(iterable, condition, default=None):
    try:
        return next(x for x in iterable if condition(x))
    except StopIteration:
        if default is not None and condition(default):
            return default
        else:
            return None

async def benchmark_time(x:Callable, logger:Callable) -> any:
    start = time.time_ns()
    result = await x()
    end = time.time_ns()
    logger(round((end-start)/1000000))
    return result