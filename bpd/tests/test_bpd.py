import unittest

import bpd
from bpd import _DASK_, _SPARK_, cfg
from bpd.dask import DaskFrame
from bpd.pyspark import PySparkDataFrame
from gnutools.remote import download_and_unzip


class TestBPD(unittest.TestCase):
    def test_download_gdrive_PySparkDataFrame(self):
        bpd.setmode(_SPARK_)
        PySparkDataFrame(download_and_unzip(cfg.gdrive.diabetes)[0]).show()

    def test_download_gdrive_DaskFrame(self):
        bpd.setmode(_DASK_)
        DaskFrame(download_and_unzip(cfg.gdrive.diabetes)[0]).display()


if __name__ == "__main__":
    unittest.main()
