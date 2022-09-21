import unittest

import bpd
from bpd import _DASK_, _SPARK_, cfg
from bpd.dask import DaskFrame
from bpd.pyspark import PySparkDataFrame
from gnutools.remote import gdrivezip


class TestBPD(unittest.TestCase):
    def test_download_gdrive_PySparkDataFrame(self):
        bpd.setmode(_SPARK_)
        PySparkDataFrame(gdrivezip(cfg.gdrive.diabetes)[0]).show()

    def test_download_gdrive_DaskFrame(self):
        bpd.setmode(_DASK_)
        DaskFrame(gdrivezip(cfg.gdrive.diabetes)[0]).display()

    def test_notebook(self):
        from gnutools import fs
        from bpd.dask import DataFrame, udf
        from bpd.dask import functions as F
        from gnutools.remote import gdrivezip

        # Import a sample dataset
        gdrivezip("gdrive://1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE")
        df = DataFrame(
            {
                "filename": fs.listfiles(
                    "/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE", [".wav"]
                )
            }
        )
        df.compute()

        # Register a user-defined function
        @udf
        def word(f):
            return fs.name(fs.parent(f))

        # Apply a udf function
        df.withColumn("classe", word(F.col("filename"))).compute()

        # You can use inline udf functions
        df.withColumn("name", udf(fs.name)(F.col("filename"))).display()

        # Retrieve the first 3 filename per classe
        df.withColumn("classe", word(F.col("filename"))).aggregate("classe").withColumn(
            "filename", F.top_k(F.col("filename"), 3)
        ).explode("filename").compute()

        # Add the classe column to the original dataframe
        df = df.withColumn("classe", word(F.col("filename")))

        # Display the modified dataframe
        df.display()

        # Display the dataframe
        # Retrieve the first 3 filename per classe
        @udf
        def initial(classe):
            return classe[0]

        _df = (
            df.aggregate("classe")
            .reset_index()
            .withColumn("initial", initial(F.col("classe")))
            .select(["classe", "initial"])
            .set_index("classe")
        )

        # Display the dataframe grouped by classe
        _df.compute()

        _df_initial = _df.reset_index().aggregate("initial")
        _df_initial.compute()

        # Join the dataframes
        df.join(_df, on="classe").drop_column("classe").join(
            _df_initial, on="initial"
        ).display()


if __name__ == "__main__":
    unittest.main()
