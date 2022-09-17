try:
    from pyspark.sql import SparkSession
    from bpd import _APP_NAME_, _DEFAULT_BACKEND_, _SPARK_
    if _DEFAULT_BACKEND_ == _SPARK_:
        spark = SparkSession.builder\
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        from pyspark.sql.functions import *
except:
    pass

try:
    from distributed import Client
    from bpd import _DASK_ENDPOINT_, _DEFAULT_BACKEND_, _DASK_
    if _DEFAULT_BACKEND_ == _DASK_:
        client = Client()
except:
    pass

if __name__=="__main__":
    from pyspark.sql import SparkSession
    spark = SparkSession.builder\
    .appName("bpd")\
    .getOrCreate()