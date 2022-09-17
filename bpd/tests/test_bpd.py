import unittest
import bpd
from bpd import _SPARK_, cfg
from bpd.dataframe import PySparkDataFrame as DataFrame
from gnutools.remote import download_and_unzip

bpd.setmode(_SPARK_)


class TestBPD(unittest.TestCase):
    def test_download_gdrive(self):
        DataFrame(download_and_unzip(cfg.gdrive.diabetes)[0]).show()


if __name__ == "__main__":
    unittest.main()
