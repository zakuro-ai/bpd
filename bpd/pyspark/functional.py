from pyspark.sql import functions as F


def sort_array_value_by_freq_asc(table, key):
    return table.select(key)\
    .explodeAll()\
    .groupBy(key.split(".")[-1])\
    .count()\
    .sort(F.col("count").desc())
