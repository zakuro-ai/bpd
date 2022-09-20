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
                return DaskFrame(result)
            return result

    def __getattr__(self, name):
        try:
            return object.__getattr__(self, name)
        except Exception:
            result = self._cdata.__getattr__(name)
            if type(result) in [dd.DataFrame]:
                return DaskFrame(result)
            return result

    def display(self, *args, **kwargs):
        return self._cdata.compute(*args, **kwargs)

    @staticmethod
    def from_pandas(df, npartitions=8):
        return DaskFrame(df, npartitions=npartitions)

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
        return DaskFrame(self.withColumn(c1, F.col(c0)).drop(c0, axis=1))

    def add_timestamp(self, idx=None):
        now = datetime.now()
        if idx is None:
            self._cdata["modified_on"] = lambda r: now
        else:
            self._cdata[idx]["modified_on"] = lambda r: now
        return self

    def aggregate(self, col, *args, **kwargs):
        return DaskFrame(self.groupby(col).agg(list).reset_index())

    def limit(self, n):
        return DaskFrame(self.loc[0:n, :])

    def join(self, df, on, *args, **kwargs):
        try:
            return DaskFrame(self.merge(df._cdata, on=on, *args, **kwargs))
        except Exception:
            return self.join(DaskFrame(df), on=on, *args, **kwargs)

    def drop_column(self, c):
        return DaskFrame(self._cdata.drop(c._object, axis=1))

    def drop_columns(self, *cols):
        if len(cols) > 1:
            return self.drop_column(cols[0]).drop_columns(*cols[1:])
        return self.drop_column(cols[0])

    def export(self, path, index=False, *args, **kwargs):
        self.compute().to_csv(path, index=index, *args, **kwargs)
        return self

    #     def select(self, *cols):
    #         return DaskFrame(self._cdata[list(cols)])
    def select(self, *cols):
        if len(cols) == 1:
            return DaskFrame(self._cdata[cols[0]])
        else:
            return DaskFrame(self._cdata[list(cols)])

    def to_list(self, *cols):
        cols = cols if len(cols) > 0 else self.columns
        return self.select(*cols).compute()._values

    def find(self, cond):
        return DaskFrame(self.loc[cond])

    # def rebase(self, d, key):
    #     d1 = self.find(~(self[key] == d.key))
