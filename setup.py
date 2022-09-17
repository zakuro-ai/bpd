from setuptools import setup
from bpd import __version__


setup(
    name="bpd",
    version=__version__,
    short_description="bpd",
    long_description="bpd",
    packages=[
        "bpd",
        "bpd.backend",
        "bpd.dataframe",
        "bpd.dataframe.backend",
        "bpd.dataframe.backend.pyspark",
        "bpd.dataframe.backend.dask",
        "bpd.dataframe.backend.pandas",
        "bpd.udf",
    ],
    include_package_data=True,
    package_data={"": ["*.yml"]},
    url="https://github.com/JeanMaximilienCadic/bpd",
    license="MIT",
    author="CADIC Jean-Maximilien",
    python_requires=">=3.6",
    install_requires=[r.rsplit()[0] for r in open("requirements.txt")],
    author_email="git@cadic.jp",
    description="bpd",
    platforms="linux_debian_10_x86_64",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
    ],
)
