from datetime import datetime

import pandas as pd
from bpd.dask import functions as F
from bpd.dataframe import DataFrame
from gnutools import fs

import dask.dataframe as dd


class DaskFrame(DataFrame):
    def __init__(
        self,
        input,
        npartitions=8,
        *args,
        **kwargs,
    ):
        self._npartitions = npartitions
        if type(input) in [dd.DataFrame]:
            self._cdata = input
        elif type(input) in [pd.DataFrame]:
            self._cdata = dd.from_pandas(input, npartitions=self._npartitions)
        elif type(input) == list:
            self._cdata = dd.from_pandas(
                pd.DataFrame.from_records({"entry": input}),
                npartitions=self._npartitions,
            )
        elif type(input) == dict:
            self._cdata = dd.from_pandas(
                pd.DataFrame.from_records(input), npartitions=npartitions
            )
        else:
            try:
                self._cdata = dd.read_csv(input)
            except Exception:
                try:
                    self._cdata = dd.from_pandas(
                        pd.DataFrame.from_records({"filename": fs.listfiles(input)}),
                        npartitions=npartitions,
                    )
                except Exception:
                    try:
                        self._cdata = input._cdata
                    except Exception:
                        self._cdata = input

    def __getattribute__(self, name):
        try:
            return object.__getattribute__(self, name)
        except Exception:
            result = self._cdata.__getattribute__(name)
            if type(result) in [dd.DataFrame]:
                self._cdata = result
                return self
            return result

    def __getattr__(self, name):
        try:
            return object.__getattr__(self, name)
        except Exception:
            result = self._cdata.__getattr__(name)
            if type(result) in [dd.DataFrame]:
                self._cdata = result
                return self
            return result

    def display(self, *args, **kwargs):
        return self.collect().compute(*args, **kwargs)
    
    def collect(self):
        self._cdata = DaskFrame(self._cdata.compute(*args, **kwargs))
        return self
    
    @staticmethod
    def from_pandas(df, npartitions=8):
        return DaskFrame(df, npartitions=npartitions)

    @staticmethod
    def from_records(records, npartitions=8):
        return DaskFrame(pd.DataFrame.from_records(records), npartitions=npartitions)

    @staticmethod
    def apply_function(r, fun, *args, **kwargs):
        _args = ()
        for arg in args:
            try:
                _args += (r[arg._object],)
            except:
                _args += (arg,)
        _kwargs = {}
        for k, v in kwargs.items():
            try:
                _kwargs.setdefault(k, v._object)
            except:
                _kwargs.setdefault(k, v)
        return fun(*_args, **_kwargs)

    def withColumn(self, column, udf_fun):
        _self = self.copy()
        try:
            _self._cdata[column] = udf_fun._object
        except:
            fun, args, kwargs, dtype = udf_fun
            _self._cdata[column] = lambda r: r.apply(
                DaskFrame.apply_function,
                args=(fun,) + args,
                **kwargs,
                meta=(column, dtype),
                axis=1,
            )
        return _self

    def withColumnRenamed(self, c0, c1):
        return self.withColumn(c1, F.identity(F.col(c0))).drop_column(c0)

    def add_timestamp(self, idx=None):
        now = datetime.now()
        if idx is None:
            self._cdata["modified_on"] = lambda r: now
        else:
            self._cdata[idx]["modified_on"] = lambda r: now
        return self

    def aggregate(self, col, args=(), kwargs={}, type_agg=None):
        _self = DaskFrame(self.groupby(col).agg(list))
        if type_agg is not None:
            for c in _self.columns:
                _self = _self.withColumn(
                    c, F.apply(type_agg, F.col(c), args=args, kwargs=kwargs)
                )
        return _self

    def limit(self, n):
        self._cdata = self._cdata.loc[0:n, :]
        return self

    def join(self, df, *args, **kwargs):
        try:
            return DaskFrame(self.copy()._cdata.merge(df._cdata, *args, **kwargs))
            # return DaskFrame(self._cdata.merge(df._cdata, *args, **kwargs))
        except Exception:
            return self.join(DaskFrame(df)._cdata, *args, **kwargs)

    def drop_column(self, c=None):
        if c is not None:
            self._cdata = self._cdata.drop(c, axis=1)
        return self

    def drop_columns(self, *cols):
        return (
            self.drop_column(cols[0]).drop_columns(*cols[1:])
            if len(cols) > 1
            else self.drop_column(cols[0])
        )

    def export(self, path, index=False, *args, **kwargs):
        self.compute().to_csv(path, index=index, *args, **kwargs)
        return self

    #     def select(self, *cols):
    #         return DaskFrame(self._cdata[list(cols)])
    def select(self, *cols):
        return (
            DaskFrame(self.copy()._cdata[cols[0]])
            if len(cols) == 1
            else DaskFrame(self.copy()._cdata[list(cols)])
        )

    def to_list(self, *cols):
        cols = cols if len(cols) > 0 else self.columns
        return self.select(*cols).compute()._values

    def find(self, cond):
        """_summary_

        Args:
            cond (_type_): Return a copy of the filter result. Doesnt affect cdata.

        Returns:
            _type_: _description_
        """
        return self.copy().filter(cond)

    def reset_index(self, hard=True, *args, **kwargs):
        if hard:
            self._cdata = (
                self._cdata.repartition(1)
                .reset_index(True)
                .repartition(npartitions=self._npartitions)
            )
        else:
            self._cdata = self._cdata.reset_index(*args, **kwargs)
        return self

    def set_index(self, *args, **kwargs):
        _self = self.copy()
        _self._cdata = _self._cdata.set_index(*args, **kwargs)
        return _self

    def copy(self, *args, **kwargs):
        return DaskFrame(self._cdata.copy())

    def filter(self, cond):
        """_summary_

        Args:
            cond (_type_): Modify cdata immediately

        Returns:
            _type_: _description_
        """
        self._cdata = self._cdata.loc[cond]
        return self

    def explode(self, *args, **kwargs):
        _self = self.copy()
        _self._cdata = _self._cdata.explode(*args, **kwargs)
        return _self

    def run_pipeline(self, pipeline, group_on=None, select_cols=()):
        # Remove the columns that will be generated in the pipeline
        try:
            for col, _ in pipeline:
                self = self.drop_column(col)
        except:
            pass
        # Groupby
        if group_on is not None:
            select_cols = tuple([col for col in select_cols if not col == group_on])
            selfg = (
                self.aggregate(group_on)
                .reset_index(hard=False)
                .select([group_on] + list(select_cols))
            )

            # Add sequential pipeline
            for col, f in pipeline:
                selfg = selfg.withColumn(col, f)

            self._cdata = (
                self.drop_columns(*select_cols)
                .join(selfg.drop_columns(*select_cols), on=group_on)
                ._cdata
            )
        else:
            # Add sequential pipeline
            for col, f in pipeline:
                self._cdata = self.withColumn(col, f)._cdata
        return self

    def run_pipelines(self, pipelines):
        for pipeline in pipelines:
            self.run_pipeline(**pipeline)
        return self
