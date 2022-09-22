import os
import unittest

from bpd.tests import *
from gnutools.fs import parent
from gnutools.tests import test_imports

if __name__ == "__main__":
    # Test imports
    test_imports(parent(os.path.realpath(__file__), level=2))
    unittest.main()
