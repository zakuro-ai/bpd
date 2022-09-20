import pandas
import pyspark
from bpd.dataframe.backend.dataframe import DataFrame
import logging
from pyspark.sql import functions as F
from pyspark.ml.feature import StringIndexer
import pathlib
from pyspark.sql.types import StructType
from bpd.pyspark.udf import try_string_get_item


class PySparkDataFrame(DataFrame):
    def __init__(
        self,
        input,
        config={},
        cache=False,
        persist=False,
        usecols=None,
        *args,
        **kwargs,
    ):
        # Init the backend
        from bpd import _SPARK_

        config["backend"] = _SPARK_
        super(PySparkDataFrame, self).__init__(input=input, **config)
        # Check the spark connexion
        try:
            assert config["spark"] is not None
        except Exception:
            try:
                from bpd.backend import spark

                self._spark = spark
            except Exception:
                logging.warning("Could not import spark...")
                raise Exception
        else:
            self._spark = config["spark"]
        self._sc = self._spark.sparkContext
        # Load
        if type(input) == str:
            prefix = "/dbfs"
            n = len(prefix)
            input = input[n:] if input.startswith(prefix) else input
            ext = pathlib.Path(input).suffix
            if ext == ".csv":
                self._cdata = (
                    self._spark.read.option("header", True)
                    .csv(input, *args, **kwargs)
                    .drop(F.col("_c0"))
                )
            elif ext == ".json":
                self._cdata = self._spark.read.json(input, *args, **kwargs).drop(
                    F.col("_c0")
                )
        elif type(input) == pandas.core.frame.DataFrame:
            self._cdata = spark.createDataFrame(input)
        else:
            self._cdata = input
        if usecols is not None:
            self._cdata = self._cdata.select(*usecols)
        # Persist
        if persist:
            self._cdata = self._cdata.persist()
        elif cache:
            self._cdata = self._cdata.cache()

    def withCollapsedColumn(self, on, attr="_collapsed", include=None):
        on = set(on) if not type(on) == str else {on}
        cdata = self._cdata
        columns = list(set(cdata.columns).difference(set(on)))
        columns += include if include is not None else []
        schema = cdata.select(*list(columns)).schema

        @F.udf(StructType(schema))
        def _Object(*values):
            return dict(zip(columns, values))

        cdata = cdata.withColumn(attr, _Object(*tuple(columns))).select(
            *(tuple(on) + (attr,))
        )

        return PySparkDataFrame(cdata)

    def to_csv(self, output_file, mode="overwrite", *args, **kwargs):
        self._cdata.write.mode(mode).csv(output_file, *args, **kwargs)

    def to_json(self, output_file, mode="overwrite", *args, **kwargs):
        self._cdata.write.mode(mode).json(output_file, *args, **kwargs)

    def select(self, *cols):
        return PySparkDataFrame(self._cdata.select(list(cols)))

    def groupby(self, *args, method=None, **kwargs):
        """_summary_

        Args:
            method (_type_, optional): _description_. Defaults to None.

        Returns:
            _type_: _description_

        Yields:
            _type_: _description_
        """
        if method == "pandas":
            for k, v in self.toPandas().groupby(*args, **kwargs):
                yield k, PySparkDataFrame(v)
        elif method == "native":
            return self.filterBy(*args, **kwargs)
        else:
            return self._cdata.groupBy(*args, **kwargs)

    # def groupByIter(self, *cols, on=None, exploded=True):
    #     df = self._cdata
    #     cols = set(cols)
    #     columns = df.columns
    #     columns_left = (
    #         list(set(columns).difference(cols)) if on is None else list(set(on))
    #     )
    #     cols = tuple(list(cols))
    #     aggs = tuple()
    #     for c in columns_left:
    #         aggs = aggs + (F.collect_list(c).alias(c),)
    #     df_agg = df.groupby(*cols).agg(*aggs)
    #     df_agg.__len__ = df_agg.count()
    #     for keys in df_agg.select(*cols).distinct().collect():
    #         conditions = None
    #         for k, v in keys.asDict().items():
    #             if conditions is None:
    #                 conditions = F.col(k) == v
    #             else:
    #                 conditions &= F.col(k) == v
    #         _df = df_agg.where(conditions)
    #         if exploded:
    #             columns_exploded = set(cols)
    #             columns_set = set(_df.columns).difference(columns_exploded)
    #             while len(columns_set) > 0:
    #                 c0 = columns_set.pop()
    #                 _df = _df.select(
    #                     *(tuple(columns_set) + tuple(columns_exploded)),
    #                     F.explode(_df[c0]).alias(c0),
    #                 )
    #                 columns_exploded.add(c0)
    #         yield keys, PySparkDataFrame(_df)

    def filterBy(self, *cols):
        df = self._cdata
        rows = df.select(*cols).distinct().collect()
        for row in rows:
            conditions = None
            cnds = row.asDict()
            for k, v in cnds.items():
                if conditions is not None:
                    conditions = conditions & (F.col(k) == v)
                else:
                    conditions = F.col(k) == v
            cnds = tuple(cnds.values())
            yield cnds if len(cnds) > 1 else cnds[0], PySparkDataFrame(
                df.where(conditions)
            )

    def withColumnRenamed(self, *args, **kwargs):
        return PySparkDataFrame(self._cdata.withColumnRenamed(*args, **kwargs))

    def drop(self, *args, **kwargs):
        return PySparkDataFrame(self._cdata.drop(*args, **kwargs))

    def toPandas(self):
        return self._cdata.toPandas()

    def describe(self, *args, **kwargs):
        return PySparkDataFrame(self._cdata.describe(*args, **kwargs))

    def where(self, *args, **kwargs):
        return PySparkDataFrame(self._cdata.where(*args, **kwargs))

    def withColumn(self, *args, **kwargs):
        return PySparkDataFrame(self._cdata.withColumn(*args, **kwargs))

    def astype(self, kv={}, *args, **kwargs):
        cdata = self._cdata
        for k, v in kv.items():
            cdata = cdata.withColumn(k, cdata[k].cast(v))
        self._cdata = cdata
        return self

    def join(self, *args, **kwargs):
        return PySparkDataFrame(self._cdata.join(*args, **kwargs))

    def distinct(self, *args, **kwargs):
        return PySparkDataFrame(self._cdata.distinct(*args, **kwargs))

    def __getattribute__(self, name):
        try:
            return object.__getattribute__(self, name)
        except Exception:
            result = self._cdata.__getattribute__(name)
            if type(result) in [pyspark.sql.dataframe.DataFrame]:
                return PySparkDataFrame(result)
            return result

    def __getattr__(self, name):
        try:
            return object.__getattr__(self, name)
        except Exception:
            result = self._cdata.__getattr__(name)
            if type(result) in [pyspark.sql.dataframe.DataFrame]:
                return PySparkDataFrame(result)
            elif type(result) in [pyspark.sql.column.Column]:
                return PySparkDataFrame(self.__getitem__(result))
            return result

    def __getitem__(self, *args, **kwargs):
        if (type(args[0]) == tuple) & (len(args) == 1):
            return self.__getitem__(*args[0])
        return PySparkDataFrame(self._cdata.select(*args))

    def __len__(self, *args, **kwargs):
        return self._cdata.count(*args, **kwargs)

    def unique(self, *cols):
        cols = cols if len(cols) > 0 else tuple(self.columns)
        return PySparkDataFrame(self.__getitem__(*cols).distinct())

    def limit(self, *args, **kwargs):
        return PySparkDataFrame(self._cdata.limit(*args, **kwargs))

    def sql(self, *args, table_name="SELF", display=True, **kwargs):
        from bpd.backend import spark

        self._cdata.createOrReplaceTempView(table_name)
        df = PySparkDataFrame(spark.sql(*args, **kwargs))
        if display:
            import bpd

            bpd.display(df)
        return df

    def toNumeric(
        self, input_col=None, output_col=None, replace=False, *args, **kwargs
    ):
        if input_col is None:
            STR = "string"
            input_cols = [k for k, v in dict(self.dtypes).items() if v == STR]
            df = self
            for input_col in input_cols:
                df = df.toNumeric(input_col=input_col, replace=replace)
            return df
        else:
            output_col = f"{input_col}_idx"
            df = self.fillna("Null", subset=[input_col])
            is_cached = df.is_cached
            indexer = StringIndexer(inputCol=input_col, outputCol=output_col)
            df = PySparkDataFrame(indexer.fit(df).transform(df))
            df.cache() if is_cached else None
            if replace:
                df = (
                    df.drop(F.col(input_col))
                    .withColumn(input_col, F.col(output_col))
                    .drop(F.col(output_col))
                )
            return df

    def display(self, *args, **kwargs):
        from bpd import display as _display

        return _display(self, *args, **kwargs)

    def aggregate(self, on, method=F.collect_set, aggs=None, **kwargs):
        """_summary_

        Args:
            on (_type_): _description_
            method (_type_, optional): _description_. Defaults to F.collect_set.
            aggs (_type_, optional): _description_. Defaults to None.

        Returns:
            _type_: _description_
        """
        on = set(on) if not type(on) == str else {on}
        if aggs is None:
            columns = set(self.columns).difference(set(on))
            aggs = tuple([method(c).alias(c) for c in columns])
        return PySparkDataFrame(self._cdata.groupBy(list(on)).agg(*aggs))

    def dropna(self, *args, **kwargs):
        return PySparkDataFrame(self._cdata.dropna(*args, **kwargs))

    def explodeAll(self, *args, **kwargs):
        _ARRAY_ = "array<"
        while len(list(set([c for c, d in self.dtypes if _ARRAY_ in d]))) > 0:
            self._cdata = self.flatten(all=True)._cdata
        return self

    def flatten(self, all=False):
        # Find the list of columns to explode
        _df = PySparkDataFrame(self)
        _ARRAY_ = "array<"
        columns_array = list(set([c for c, d in _df.dtypes if _ARRAY_ in d]))
        if not all:
            aggs = tuple()
            for c in columns_array:
                _df = _df.withColumn(f"_{c}", F.size(c))
                aggs += (F.max(f"_{c}").alias(f"_{c}"),)
            columns_to_explode = [
                k[1:] for k, v in _df.agg(*aggs).collect()[0].asDict().items() if v == 1
            ]
            # Remove the count columns
            for c in columns_array:
                _df = _df.drop(F.col(f"_{c}"))
        else:
            columns_to_explode = columns_array
        # Explode the columns
        for c in columns_to_explode:
            _df = _df.withColumn(c, F.explode(c))
        return _df

    def fillna(self, *args, **kwargs):
        return PySparkDataFrame(self._cdata.fillna(*args, **kwargs))

    def filter(self, *args, **kwargs):
        return PySparkDataFrame(self._cdata.filter(*args, **kwargs))

    def remove_features(self, *cols):
        for c in cols:
            self = self.drop(F.col(c))
        return self

    def add_array_size(self, c):
        return self.withColumn(f"{c}_size", F.size(c))

    # def self_sorted(self, c):
    #     return self.withColumn(c, filtered_sorted_array_string(F.col(c), F.lit(c)))

    def add_top_k(self, c, k):
        for _k in range(k):
            self = self.withColumn(f"{c}_{k}", try_string_get_item(c, F.lit(k)))
        return self

    def split_date(self, c):
        return (
            self.withColumn(f"{c}_year", F.year(F.col(c)))
            .withColumn(f"{c}_month", F.month(F.col(c)))
            .withColumn(f"{c}_day", F.dayofmonth(F.col(c)))
        )
