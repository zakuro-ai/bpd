from bpd.dask import types


def contains(f, value):
    return f.__contains__(value)


def identity(self, v):
    return v


def col(v):
    return types.col(v)


def lit(v):
    return types.lit(v)


def apply(f, c, args=()):
    return types.apply(f, c, args)
