import pyspark
import pandas
import dask
from bpd.pyspark import PySparkDataFrame
from bpd.dask import DaskDataFrame, DaskFrame
from bpd.pandas import PandasDataFrame

class DataFrame:
    def __init__(self, input, **config):
        from bpd.dataframe import PySparkDataFrame, DaskDataFrame, PandasDataFrame

        # assert config.__contains__("backend")
        assert type(input) in [
            str,  # read from fs
            PySparkDataFrame,  # PySpark dataframe
            DaskDataFrame,  # Dask dataframe
            DaskFrame,  # Dask like PySpark dataframe
            PandasDataFrame,  # Pandas dataframe
            pyspark.sql.dataframe.DataFrame,  # PySparkSQL native dataframe
            pandas.core.frame.DataFrame,  # Pandas native dataframe
            dask.dataframe.core.DataFrame,  # Dask native dataframe
        ]
        self._backend = config["backend"]

    def __getattribute__(self, name):
        try:
            return object.__getattribute__(self, name)
        except:
            return self._cdata.__getattribute__(name)

    def __getattr__(self, name):
        try:
            return object.__getattr__(self, name)
        except:
            return self._cdata.__getattr__(name)

    def __len__(self, *args, **kwargs):
        try:
            return object.__len__(*args, **kwargs)
        except:
            result = self._cdata.__len__(*args, **kwargs)
            return result
