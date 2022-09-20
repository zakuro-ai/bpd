try:
    from bpd import _DASK_, _DASK_ENDPOINT_, _DEFAULT_BACKEND_

    if _DEFAULT_BACKEND_ == _DASK_:
        from distributed import Client

        client = Client()
except:
    pass
