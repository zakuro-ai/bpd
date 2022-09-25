import logging

import pandas as pd

from bpd.dataframe import DataFrame


class PandasDataFrame(DataFrame):
    def __init__(
        self,
        input,
        config={},
        cache=False,
        persist=False,
        usecols=None,
        *args,
        **kwargs
    ):
        # Init the backend
        from bpd import _PANDAS_

        config["backend"] = _PANDAS_
        super(PandasDataFrame, self).__init__(input=input, **config)

        # Load
        if type(input) == str:
            input = input[len("/dbfs") :] if input.startswith("/dbfs") else input
            self._cdata = pd.read_csv(input, usecols=usecols, *args, **kwargs)
        elif type(input) == pd.core.frame.DataFrame:
            self._cdata = input

        # Persist
        if persist:
            logging.warning("Persist is ignored in Pandas context...")
        elif cache:
            logging.warning("Cache is ignored in Pandas context...")

    def show(self, *args, **kwargs):
        print(self.head(*args, **kwargs))

    def __getattribute__(self, name):
        try:
            return object.__getattribute__(self, name)
        except:
            result = self._cdata.__getattribute__(name)
            if type(result) in [pd.core.frame.DataFrame]:
                return PandasDataFrame(result)
            return result

    def __getattr__(self, name):
        try:
            return object.__getattr__(self, name)
        except:
            result = self._cdata.__getattr__(name)
            if type(result) in [pd.core.frame.DataFrame]:
                return PandasDataFrame(result)
            return result

    def __len__(self, *args, **kwargs):
        try:
            return object.__len__(*args, **kwargs)
        except:
            result = self._cdata.__len__(*args, **kwargs)
            if type(result) in [pd.core.frame.DataFrame]:
                return PandasDataFrame(result)
            return result

    def __getitem__(self, *args, **kwargs):
        try:
            return object.__getitem__(self, *args, **kwargs)
        except:
            result = self._cdata.__getitem__(*args, **kwargs)
            if type(result) in [pd.core.frame.DataFrame]:
                return PandasDataFrame(result)
            return result

