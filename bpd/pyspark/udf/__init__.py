from datetime import datetime

import pyspark.sql.functions as F
from pyspark.sql.types import ArrayType, DateType, DoubleType, IntegerType, StringType


@F.udf(StringType(), True)
def try_string_get_item(c, k):
    try:
        return c[k]
    except:
        return "NaN"


@F.udf(DoubleType(), True)
def try_double_get_item(c, k):
    try:
        return c[k]
    except:
        return float("NaN")


@F.udf(IntegerType(), True)
def try_int_get_item(c, k):
    try:
        return c[k]
    except:
        return -1


@F.udf(DateType())
def to_date(d):
    return datetime.strptime(str(d), "%Y%m%d")


@F.udf(ArrayType(IntegerType(), True))
def unique_int_values(c):
    return list(set(c))


@F.udf(ArrayType(StringType(), True))
def unique_string_values(c):
    try:
        return list(set(c))
    except:
        return ["NaN"]


@F.udf(ArrayType(DoubleType(), True))
def unique_double_values(c):
    try:
        return list(set(c))
    except:
        return [float("NaN")]


@F.udf(ArrayType(DoubleType(), True))
def string_list_to_double(c):
    return list([int(v) for v in c])


@F.udf(DoubleType(), True)
def string_to_double(v):
    return float(v)


@F.udf(StringType(), True)
def string_get_item(c, k):
    return c[k]


@F.udf(DoubleType(), True)
def double_get_item(c, k):
    return c[k]


@F.udf(IntegerType(), True)
def int_get_item(c, k):
    return c[k]


@F.udf(DateType(), True)
def date_get_item(c, k):
    return c[k]
