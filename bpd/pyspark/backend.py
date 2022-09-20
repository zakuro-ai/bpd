try:
    from bpd import _APP_NAME_, _DEFAULT_BACKEND_, _SPARK_
    if _DEFAULT_BACKEND_ == _SPARK_:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder\
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        from pyspark.sql.functions import *
except:
    pass
