
<h1 align="center">
  <br>
  <img src="https://drive.google.com/uc?id=1CV1tY4jcZDO4g_CLhGQK5VUN2q9SNsll" width="200">
  <br>
    bpd
  <br>
</h1>

<p align="center">
  <a href="#modules">Modules</a> •
  <a href="#code-structure">Code structure</a> •
  <a href="#installing-the-application">Installing the application</a> •
  <a href="#makefile-commands">Makefile commands</a> •
  <a href="#environments">Environments</a> •
  <a href="#running-the-application">Running the application</a>•
  <a href="#notebook">Notebook</a>•
  <a href="#ressources">Ressources</a>
</p>


# Code structure
```python
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


```


# Installing the application
To clone and run this application, you'll need the following installed on your computer:
- [Git](https://git-scm.com)
- Docker Desktop
   - [Install Docker Desktop on Mac](https://docs.docker.com/docker-for-mac/install/)
   - [Install Docker Desktop on Windows](https://docs.docker.com/desktop/install/windows-install/)
   - [Install Docker Desktop on Linux](https://docs.docker.com/desktop/install/linux-install/)
- [Python](https://www.python.org/downloads/)

Install bpd:
```bash
# Clone this repository and install the code
git clone https://github.com/JeanMaximilienCadic/bpd

# Go into the repository
cd bpd
```


# Makefile commands
Exhaustive list of make commands:
```
install_wheels
sandbox_cpu
sandbox_gpu
build_sandbox
push_environment
push_container_sandbox
push_container_vanilla
pull_container_vanilla
pull_container_sandbox
build_vanilla
build_wheels
auto_branch 
```
# Environments

## Docker

> **Note**
> 
> Running this application by using Docker is recommended.

To build and run the docker image
```
make build
make sandbox
```

## PythonEnv

> **Warning**
> 
> Running this application by using PythonEnv is possible but *not* recommended.
```
make install_wheels
```
# Running the application

```console
make tests
```
```
=1= TEST PASSED : bpd
=1= TEST PASSED : bpd.dask
=1= TEST PASSED : bpd.dask.types
=1= TEST PASSED : bpd.pandas
=1= TEST PASSED : bpd.pyspark
=1= TEST PASSED : bpd.pyspark.udf
=1= TEST PASSED : bpd.tests
+-----------+-------+-------------+-------------+-------+----+------------------------+---+-------+
|Pregnancies|Glucose|BloodPressure|SkinThickness|Insulin| BMI|DiabetesPedigreeFunction|Age|Outcome|
+-----------+-------+-------------+-------------+-------+----+------------------------+---+-------+
|          6|    148|           72|           35|      0|33.6|                   0.627| 50|      1|
|          1|     85|           66|           29|      0|26.6|                   0.351| 31|      0|
|          8|    183|           64|            0|      0|23.3|                   0.672| 32|      1|
|          1|     89|           66|           23|     94|28.1|                   0.167| 21|      0|
|          0|    137|           40|           35|    168|43.1|                   2.288| 33|      1|
|          5|    116|           74|            0|      0|25.6|                   0.201| 30|      0|
|          3|     78|           50|           32|     88|  31|                   0.248| 26|      1|
|         10|    115|            0|            0|      0|35.3|                   0.134| 29|      0|
|          2|    197|           70|           45|    543|30.5|                   0.158| 53|      1|
|          8|    125|           96|            0|      0|   0|                   0.232| 54|      1|
|          4|    110|           92|            0|      0|37.6|                   0.191| 30|      0|
|         10|    168|           74|            0|      0|  38|                   0.537| 34|      1|
|         10|    139|           80|            0|      0|27.1|                   1.441| 57|      0|
|          1|    189|           60|           23|    846|30.1|                   0.398| 59|      1|
|          5|    166|           72|           19|    175|25.8|                   0.587| 51|      1|
|          7|    100|            0|            0|      0|  30|                   0.484| 32|      1|
|          0|    118|           84|           47|    230|45.8|                   0.551| 31|      1|
|          7|    107|           74|            0|      0|29.6|                   0.254| 31|      1|
|          1|    103|           30|           38|     83|43.3|                   0.183| 33|      0|
|          1|    115|           70|           30|     96|34.6|                   0.529| 32|      1|
+-----------+-------+-------------+-------------+-------+----+------------------------+---+-------+
only showing top 20 rows

.
----------------------------------------------------------------------
Ran 1 test in 2.701s

OK
```

# Notebook
```python
from gnutools import fs
from bpd.dask import DataFrame
from bpd.dask import functions as F
from gnutools.remote import gdrivezip
```


```python
# Define a function to be applied to each row
def word(f):
    return fs.name(fs.parent(f))
```


```python
# Import a sample dataset
gdrivezip("gdrive://1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE")
df = DataFrame({"filename": fs.listfiles("/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE", [".wav"])})
```


```python
# Chain transformations
df = df\
    .withColumn("name", F.apply(fs.name, F.col("filename")))\
    .withColumn("word", F.apply(word, F.col("filename")))\
    .set_index("word")
```


```python
#Display the dataframe
df.display()
```


```python
# Define a function to be applied to a groupby
def first(L):
    return L[0]
```


```python
# Join to the dataframe grouped by word. 
# The aggregation with generate a list of files for each unique word.
# Add a column `first` that will contain the result of the function first given the input of the filename column
# Drop the colummn filename 
# Join on word, right
# Reset the index and display
df.join(df\
        .select(["filename"])\
        .aggregate("word")\
        .withColumn("first", F.apply(first, F.col("filename")))\
        .drop_column(F.col("filename")), on="word", how="right"
       )\
.reset_index().display()
```


## Ressources
* Vanilla:  https://en.wikipedia.org/wiki/Vanilla_software
* Sandbox: https://en.wikipedia.org/wiki/Sandbox_(software_development)
* All you need is docker: https://www.theregister.com/2014/05/23/google_containerization_two_billion/
* Dev in containers : https://code.visualstudio.com/docs/remote/containers
* Delta lake partitions: https://k21academy.com/microsoft-azure/data-engineer/delta-lake/
