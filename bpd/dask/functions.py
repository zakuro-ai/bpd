from bpd.dask import types


def at_position(f, k):
    return f[k]


def first(f, *args, **kwargs):
    return at_position(f, k=0)


def last(f, *args, **kwargs):
    return at_position(f, k=-1)


def contains(f, value, *args, **kwargs):
    return f.__contains__(value)


def identity(self, v, *args, **kwargs):
    return v


def col(v, *args, **kwargs):
    return types.col(v)


def lit(v, *args, **kwargs):
    return types.lit(v)


def apply(f, c, args=(), kwargs={}):
    return types.apply(f, c, args=args, kwargs=kwargs)
