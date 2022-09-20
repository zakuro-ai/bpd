class col(object):
    def __init__(self, object):
        self.dtype = "col"
        self._object = object


class lit(object):
    def __init__(self, object):
        self.dtype = "lit"
        self._object = object


class apply(object):
    def __init__(self, f, c, args):
        self.dtype = "apply"
        self._f = f
        self._column = c
        self._args = args
