from gnutools.fs import load_config, parent

cfg = load_config(f"{parent(__file__)}/config.yml")
__version__ = "2.0.0"
_SPARK_ = cfg.spark
_DASK_ = cfg.dask
_PANDAS_ = cfg.pandas
_DASK_ENDPOINT_ = cfg.dask_endpoint
_APP_NAME_ = cfg.project
_DEFAULT_BACKEND_ = cfg.default_backend


def setmode(_backend):
    assert _backend in [_SPARK_, _DASK_, _PANDAS_]
    global _DEFAULT_BACKEND_
    _DEFAULT_BACKEND_ = _backend
    if _DEFAULT_BACKEND_ == _DASK_:
        from bpd.dask.backend import client

        return client
    elif _DEFAULT_BACKEND_ == _SPARK_:
        from bpd.pyspark.backend import spark

        return spark


def read_json(
    filepath_or_buffer, persist=False, cache=False, usecols=None, *args, **kwargs
):
    global _DEFAULT_BACKEND_
    assert _DEFAULT_BACKEND_ in [_SPARK_, _DASK_, _PANDAS_]
    if _DEFAULT_BACKEND_ == _SPARK_:
        from bpd.dataframe import PySparkDataFrame

        return PySparkDataFrame(
            input=filepath_or_buffer,
            config={"backend": _DEFAULT_BACKEND_},
            persist=persist,
            cache=cache,
            usecols=usecols,
            *args,
            **kwargs,
        )


def read_csv(
    filepath_or_buffer,
    persist=False,
    cache=False,
    usecols=None,
    infer_schema=True,
    *args,
    **kwargs,
):
    global _DEFAULT_BACKEND_
    assert _DEFAULT_BACKEND_ in [_SPARK_, _DASK_, _PANDAS_]
    if _DEFAULT_BACKEND_ == _SPARK_:
        from bpd.dataframe import PySparkDataFrame

        kwargs["inferSchema"] = infer_schema
        return PySparkDataFrame(
            input=filepath_or_buffer,
            config={"backend": _DEFAULT_BACKEND_},
            persist=persist,
            cache=cache,
            usecols=usecols,
            *args,
            **kwargs,
        )
    elif _DEFAULT_BACKEND_ == _DASK_:
        from bpd.dataframe import DaskDataFrame

        return DaskDataFrame(
            input=filepath_or_buffer,
            config={"backend": _DEFAULT_BACKEND_},
            persist=persist,
            cache=cache,
            #     usecols=usecols,
            *args,
            **kwargs,
        )
    else:
        from bpd.dataframe import PandasDataFrame

        return PandasDataFrame(
            input=filepath_or_buffer,
            config={"backend": _DEFAULT_BACKEND_},
            persist=persist,
            cache=cache,
            usecols=usecols,
            *args,
            **kwargs,
        )


def read_table(*args, timestamp=None, **kwargs):
    global _DEFAULT_BACKEND_
    assert _DEFAULT_BACKEND_ in [_SPARK_]
    from bpd.backend import spark
    from bpd.dataframe import PySparkDataFrame

    if timestamp is None:
        return PySparkDataFrame(spark.read.table(*args, **kwargs))
    else:
        return PySparkDataFrame(
            spark.read.option("timestampAsOf", timestamp).table(*args, **kwargs)
        )


def display(df, n=5, *args, **kwargs):
    try:
        import PythonShellImpl

        return PythonShellImpl.PythonShell.display(df._cdata)
    except:
        from IPython.display import display as Display

        return Display(df.limit(n, *args, **kwargs).toPandas())


def read_delta(*args, **kwargs):
    global _DEFAULT_BACKEND_
    assert _DEFAULT_BACKEND_ in [_SPARK_]
    from bpd.backend import spark
    from bpd.dataframe import PySparkDataFrame

    return PySparkDataFrame(spark.read.format("delta").load(*args, **kwargs))


def create_temp_views(L):
    import bpd

    missing_tables = list()
    for root, reader, db, t in L:
        if reader == bpd.read_csv:
            table = f"{root}/{db}_{t}.csv"
        elif reader == bpd.read_json:
            table = f"{root}/{db}_{t}.json"
        elif reader == bpd.read_table:
            table = f"{db}.{t}"
        else:
            table = f"{root}/{db}_{t}"
        try:
            reader(table).createOrReplaceTempView(t)
        except AnalysisException:
            missing_tables.append(table)
    return missing_tables
