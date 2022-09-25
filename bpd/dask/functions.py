import psutil

from bpd.dask import types


def udf(dtype=None):
    def map_dtype(dtype):
        if dtype in [str, None]:
            return "object"

    def multipass(func):
        def wrapper(*args, **kw):
            return func, args, kw, map_dtype(dtype)

        return wrapper

    return multipass


@udf()
def top_k(f, k):
    return f[:k]


@udf()
def last_k(f, k):
    return f[-k:]


@udf()
def at_position(f, k):
    return f[k]


@udf()
def first(f):
    return f[0]


@udf()
def last(f):
    return f[-1]


@udf()
def contains(f, value):
    return f.__contains__(value)


@udf()
def identity(v):
    return v


def col(v, *args, **kwargs):
    return types.col(v)


def lit(v, *args, **kwargs):
    return types.lit(v)


def limit_mem(percentage=100):
    def multipass(func):
        def wrapper(*args, **kw):
            if psutil.virtual_memory()[2] < percentage:
                return func(*args, **kw)
            else:
                return float("nan")

        return wrapper

    return multipass
