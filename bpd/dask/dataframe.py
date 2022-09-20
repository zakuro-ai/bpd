from bpd.dataframe.backend.dataframe import DataFrame
from dask import dataframe as dd
import dask

class DaskDataFrame(DataFrame):
  def __init__(
      self,
      input,
      config={},
      cache=False,
      persist=False,
      *args,
      **kwargs):
    from bpd import _DASK_
    config["backend"] = _DASK_
    super(DaskDataFrame, self).__init__(input=input, **config)
    if type(input)==str:
      input = input[len("/dbfs"):] if input.startswith("/dbfs") else input
      self._cdata = dd.read_csv(input, *args, **kwargs)
    else:
      self._cdata = input
    
    # Persist
    if persist:
      self._cdata.persist()
    
  def groupby(self, col):
    for lgl in self._cdata[col].unique().compute():
      yield lgl, DaskDataFrame(self.where_eq(col, lgl))
  
  def select(self, *cols):
    return DaskDataFrame(self._cdata[list(cols)])
  
  def where_eq(self, key, value):
    return self._cdata[self._cdata[key] == value]
  
  def toPandas(self):
    return self._cdata.compute()
  
  def count(self, *args, **kwargs):
    return len(self._cdata)

  def head(self, *args, **kwargs):
    return self._cdata.head(*args, **kwargs)
    
  def __getattribute__(self, name):
    try:    
        return object.__getattribute__(self, name)
    except:
        result = self._cdata.__getattribute__(name)
        if type(result) in [dask.dataframe.core.DataFrame]:
            return DaskDataFrame(result)
        return result
      
  def __getattr__(self, name):
      try:
          return object.__getattr__(self, name)
      except:
          result = self._cdata.__getattr__(name)
          if type(result) in [dask.dataframe.core.DataFrame]:
              return DaskDataFrame(result)
          return result
  def __getitem__(self, *args, **kwargs):
      try:
          return object.__getitem__(self,  *args, **kwargs)
      except:
          result = self._cdata.__getitem__(*args, **kwargs)
          if type(result) in [dask.dataframe.core.DataFrame]:
              return DaskDataFrame(result)
          return result
        
  def show(self, *args, **kwargs):
        return self.head(*args, **kwargs)
      
if __name__=="__main__":
    path = "/dbfs/FileStore/cmj-recsys/landing/sample_basket_20220124.csv"

    bdf = DaskDataFrame(path, 
    low_memory=False,
    dtype={
    'cnsmr_id_nbr': 'object',
    'cmp_external_id': 'object',
    'cmp_offer_name': 'object',
    'cmp_offer_source': 'object',
    'trigger_trade_item_cd_list': 'object'
    })

