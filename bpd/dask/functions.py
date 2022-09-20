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

          
def contains(f, value):
    return f.__contains__(value)

class F(object):
    def __init__(self):
        pass
    
    def identity(self, v):
        return v
  
    @staticmethod
    def col(v):
        return col(v)
    
    @staticmethod
    def lit(v):
        return lit(v)
    
    @staticmethod
    def apply(f, c, args=()):
        return apply(f, c, args)