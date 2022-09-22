from setuptools import setup
from bpd import __version__


setup(
    name="bpd",
    version=__version__,
    short_description="bpd",
    packages=[
        "bpd",
        "bpd.dask",
        "bpd.dask.types",
        "bpd.pandas",
        "bpd.pyspark",
        "bpd.pyspark.udf",
        "bpd.tests",
    ],
    long_description="".join(open("README.md", "r").readlines()),
    long_description_content_type="text/markdown",
    include_package_data=True,
    package_data={"": ["*.yml"]},
    url="https://github.com/zakuro-ai/bpd",
    license="MIT",
    author="ZakuroAI",
    python_requires=">=3.6",
    install_requires=[r.rsplit()[0] for r in open("requirements.txt")],
    author_email="git@zakuro.ai",
    description="bpd",
    platforms="linux_debian_10_x86_64",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
    ],
)
