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
        return self._cdata.compute(*args, **kwargs)

    @staticmethod
    def from_pandas(df, npartitions=8):
        return DaskFrame(df, npartitions=npartitions)

    @staticmethod
    def from_records(records, npartitions=8):
        return DaskFrame(pd.DataFrame.from_records(records), npartitions=npartitions)

    def withColumn(self, col_name, mapf):
        if mapf.dtype == "apply":
            self._cdata[col_name] = lambda r: r[mapf._column._object].apply(
                mapf._f, args=mapf._args, meta=(col_name, "object")
            )
        elif mapf.dtype == "lit":
            # "modified_on", F.lit(datetime.now()
            self._cdata[col_name] = lambda r: mapf._object
        elif mapf.dtype == "col":
            if mapf._object not in ["index"]:
                self._cdata[col_name] = lambda r: r[mapf._object]
            else:
                self._cdata[col_name] = lambda r: r.index
        return self

    def withColumnRenamed(self, c0, c1):
        return self.withColumn(c1, F.col(c0)).drop(c0, axis=1)

    def add_timestamp(self, idx=None):
        now = datetime.now()
        if idx is None:
            self._cdata["modified_on"] = lambda r: now
        else:
            self._cdata[idx]["modified_on"] = lambda r: now
        return self

    def aggregate(self, col, *args, **kwargs):
        self._cdata = DaskFrame(self.groupby(col).agg(list))._cdata
        return self

    def limit(self, n):
        self._cdata = self._cdata.loc[0:n, :]
        return self

    def join(self, df, *args, **kwargs):
        try:
            return DaskFrame(self.copy()._cdata.merge(df._cdata, *args, **kwargs))
        except Exception:
            return self.join(DaskFrame(df)._cdata, *args, **kwargs)

    def drop_column(self, c):
        self._cdata = self._cdata.drop(c._object, axis=1)
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

    def reset_index(self, *args, **kwargs):
        self._cdata = (
            self._cdata.repartition(1)
            .reset_index(*args, **kwargs)
            .repartition(npartitions=self.npartitions)
        )

        return self

    def set_index(self, *args, **kwargs):
        self._cdata = self._cdata.set_index(*args, **kwargs)
        return self

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
