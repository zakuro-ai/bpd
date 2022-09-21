
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
from bpd.dask import DataFrame, udf
from bpd.dask import functions as F
from gnutools.remote import gdrivezip
```


```python
# Import a sample dataset
gdrivezip("gdrive://1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE")
df = DataFrame({"filename": fs.listfiles("/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE", [".wav"])})
df.compute()      
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>filename</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE/wow/919...</td>
    </tr>
    <tr>
      <th>1</th>
      <td>/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE/wow/6a2...</td>
    </tr>
    <tr>
      <th>2</th>
      <td>/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE/wow/682...</td>
    </tr>
    <tr>
      <th>3</th>
      <td>/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE/wow/beb...</td>
    </tr>
    <tr>
      <th>4</th>
      <td>/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE/wow/d37...</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
    </tr>
    <tr>
      <th>145</th>
      <td>/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE/left/6a...</td>
    </tr>
    <tr>
      <th>146</th>
      <td>/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE/left/e3...</td>
    </tr>
    <tr>
      <th>147</th>
      <td>/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE/left/68...</td>
    </tr>
    <tr>
      <th>148</th>
      <td>/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE/left/e7...</td>
    </tr>
    <tr>
      <th>149</th>
      <td>/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE/left/65...</td>
    </tr>
  </tbody>
</table>
<p>150 rows × 1 columns</p>
</div>




```python
# Register a user-defined function
@udf
def word(f):
    return fs.name(fs.parent(f))

# Apply a udf function
df\
.withColumn("classe", word(F.col("filename")))\
.compute()    
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>filename</th>
      <th>classe</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE/wow/919...</td>
      <td>wow</td>
    </tr>
    <tr>
      <th>1</th>
      <td>/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE/wow/6a2...</td>
      <td>wow</td>
    </tr>
    <tr>
      <th>2</th>
      <td>/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE/wow/682...</td>
      <td>wow</td>
    </tr>
    <tr>
      <th>3</th>
      <td>/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE/wow/beb...</td>
      <td>wow</td>
    </tr>
    <tr>
      <th>4</th>
      <td>/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE/wow/d37...</td>
      <td>wow</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>145</th>
      <td>/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE/left/6a...</td>
      <td>left</td>
    </tr>
    <tr>
      <th>146</th>
      <td>/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE/left/e3...</td>
      <td>left</td>
    </tr>
    <tr>
      <th>147</th>
      <td>/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE/left/68...</td>
      <td>left</td>
    </tr>
    <tr>
      <th>148</th>
      <td>/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE/left/e7...</td>
      <td>left</td>
    </tr>
    <tr>
      <th>149</th>
      <td>/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE/left/65...</td>
      <td>left</td>
    </tr>
  </tbody>
</table>
<p>150 rows × 2 columns</p>
</div>




```python
# You can use inline udf functions
df\
.withColumn("name", udf(fs.name)(F.col("filename")))\
.display()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>filename</th>
      <th>name</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE/wow/919...</td>
      <td>919d3c0e_nohash_2</td>
    </tr>
    <tr>
      <th>1</th>
      <td>/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE/wow/6a2...</td>
      <td>6a27a9bf_nohash_0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE/wow/682...</td>
      <td>6823565f_nohash_2</td>
    </tr>
    <tr>
      <th>3</th>
      <td>/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE/wow/beb...</td>
      <td>beb49c22_nohash_1</td>
    </tr>
    <tr>
      <th>4</th>
      <td>/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE/wow/d37...</td>
      <td>d37e4bf1_nohash_0</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>145</th>
      <td>/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE/left/6a...</td>
      <td>6a27a9bf_nohash_0</td>
    </tr>
    <tr>
      <th>146</th>
      <td>/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE/left/e3...</td>
      <td>e32ff49d_nohash_0</td>
    </tr>
    <tr>
      <th>147</th>
      <td>/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE/left/68...</td>
      <td>6823565f_nohash_2</td>
    </tr>
    <tr>
      <th>148</th>
      <td>/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE/left/e7...</td>
      <td>e77d88fc_nohash_0</td>
    </tr>
    <tr>
      <th>149</th>
      <td>/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE/left/65...</td>
      <td>659b7fae_nohash_2</td>
    </tr>
  </tbody>
</table>
<p>150 rows × 2 columns</p>
</div>




```python
# Retrieve the first 3 filename per classe
df\
.withColumn("classe", word(F.col("filename")))\
.aggregate("classe")\
.withColumn("filename", F.top_k(F.col("filename"), 3))\
.explode("filename")\
.compute()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>filename</th>
    </tr>
    <tr>
      <th>classe</th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>wow</th>
      <td>/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE/wow/919...</td>
    </tr>
    <tr>
      <th>wow</th>
      <td>/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE/wow/6a2...</td>
    </tr>
    <tr>
      <th>wow</th>
      <td>/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE/wow/682...</td>
    </tr>
    <tr>
      <th>nine</th>
      <td>/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE/nine/0f...</td>
    </tr>
    <tr>
      <th>nine</th>
      <td>/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE/nine/6a...</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
    </tr>
    <tr>
      <th>yes</th>
      <td>/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE/yes/0a9...</td>
    </tr>
    <tr>
      <th>yes</th>
      <td>/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE/yes/0a7...</td>
    </tr>
    <tr>
      <th>left</th>
      <td>/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE/left/6a...</td>
    </tr>
    <tr>
      <th>left</th>
      <td>/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE/left/e3...</td>
    </tr>
    <tr>
      <th>left</th>
      <td>/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE/left/68...</td>
    </tr>
  </tbody>
</table>
<p>90 rows × 1 columns</p>
</div>




```python
# Add the classe column to the original dataframe
df = df\
.withColumn("classe", word(F.col("filename")))

# Display the modified dataframe
df.display()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>filename</th>
      <th>classe</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE/wow/919...</td>
      <td>wow</td>
    </tr>
    <tr>
      <th>1</th>
      <td>/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE/wow/6a2...</td>
      <td>wow</td>
    </tr>
    <tr>
      <th>2</th>
      <td>/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE/wow/682...</td>
      <td>wow</td>
    </tr>
    <tr>
      <th>3</th>
      <td>/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE/wow/beb...</td>
      <td>wow</td>
    </tr>
    <tr>
      <th>4</th>
      <td>/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE/wow/d37...</td>
      <td>wow</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>145</th>
      <td>/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE/left/6a...</td>
      <td>left</td>
    </tr>
    <tr>
      <th>146</th>
      <td>/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE/left/e3...</td>
      <td>left</td>
    </tr>
    <tr>
      <th>147</th>
      <td>/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE/left/68...</td>
      <td>left</td>
    </tr>
    <tr>
      <th>148</th>
      <td>/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE/left/e7...</td>
      <td>left</td>
    </tr>
    <tr>
      <th>149</th>
      <td>/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE/left/65...</td>
      <td>left</td>
    </tr>
  </tbody>
</table>
<p>150 rows × 2 columns</p>
</div>




```python
# Display the dataframe
# Retrieve the first 3 filename per classe
@udf
def initial(classe):
    return classe[0]
    

_df = df\
.aggregate("classe")\
.reset_index()\
.withColumn("initial", initial(F.col("classe")))\
.select(["classe", "initial"])\
.set_index("classe")

# Display the dataframe grouped by classe
_df.compute()
    
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>initial</th>
    </tr>
    <tr>
      <th>classe</th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>bed</th>
      <td>b</td>
    </tr>
    <tr>
      <th>bird</th>
      <td>b</td>
    </tr>
    <tr>
      <th>cat</th>
      <td>c</td>
    </tr>
    <tr>
      <th>dog</th>
      <td>d</td>
    </tr>
    <tr>
      <th>down</th>
      <td>d</td>
    </tr>
    <tr>
      <th>eight</th>
      <td>e</td>
    </tr>
    <tr>
      <th>five</th>
      <td>f</td>
    </tr>
    <tr>
      <th>four</th>
      <td>f</td>
    </tr>
    <tr>
      <th>go</th>
      <td>g</td>
    </tr>
    <tr>
      <th>happy</th>
      <td>h</td>
    </tr>
    <tr>
      <th>house</th>
      <td>h</td>
    </tr>
    <tr>
      <th>left</th>
      <td>l</td>
    </tr>
    <tr>
      <th>marvin</th>
      <td>m</td>
    </tr>
    <tr>
      <th>nine</th>
      <td>n</td>
    </tr>
    <tr>
      <th>no</th>
      <td>n</td>
    </tr>
    <tr>
      <th>off</th>
      <td>o</td>
    </tr>
    <tr>
      <th>on</th>
      <td>o</td>
    </tr>
    <tr>
      <th>one</th>
      <td>o</td>
    </tr>
    <tr>
      <th>right</th>
      <td>r</td>
    </tr>
    <tr>
      <th>seven</th>
      <td>s</td>
    </tr>
    <tr>
      <th>sheila</th>
      <td>s</td>
    </tr>
    <tr>
      <th>six</th>
      <td>s</td>
    </tr>
    <tr>
      <th>stop</th>
      <td>s</td>
    </tr>
    <tr>
      <th>three</th>
      <td>t</td>
    </tr>
    <tr>
      <th>tree</th>
      <td>t</td>
    </tr>
    <tr>
      <th>two</th>
      <td>t</td>
    </tr>
    <tr>
      <th>up</th>
      <td>u</td>
    </tr>
    <tr>
      <th>wow</th>
      <td>w</td>
    </tr>
    <tr>
      <th>yes</th>
      <td>y</td>
    </tr>
    <tr>
      <th>zero</th>
      <td>z</td>
    </tr>
  </tbody>
</table>
</div>




```python
_df_initial = _df.reset_index().aggregate("initial")
_df_initial.compute()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>classe</th>
    </tr>
    <tr>
      <th>initial</th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>b</th>
      <td>[bed, bird]</td>
    </tr>
    <tr>
      <th>c</th>
      <td>[cat]</td>
    </tr>
    <tr>
      <th>d</th>
      <td>[dog, down]</td>
    </tr>
    <tr>
      <th>e</th>
      <td>[eight]</td>
    </tr>
    <tr>
      <th>f</th>
      <td>[five, four]</td>
    </tr>
    <tr>
      <th>g</th>
      <td>[go]</td>
    </tr>
    <tr>
      <th>h</th>
      <td>[happy, house]</td>
    </tr>
    <tr>
      <th>l</th>
      <td>[left]</td>
    </tr>
    <tr>
      <th>m</th>
      <td>[marvin]</td>
    </tr>
    <tr>
      <th>n</th>
      <td>[nine, no]</td>
    </tr>
    <tr>
      <th>o</th>
      <td>[off, on, one]</td>
    </tr>
    <tr>
      <th>r</th>
      <td>[right]</td>
    </tr>
    <tr>
      <th>s</th>
      <td>[seven, sheila, six, stop]</td>
    </tr>
    <tr>
      <th>t</th>
      <td>[three, tree, two]</td>
    </tr>
    <tr>
      <th>u</th>
      <td>[up]</td>
    </tr>
    <tr>
      <th>w</th>
      <td>[wow]</td>
    </tr>
    <tr>
      <th>y</th>
      <td>[yes]</td>
    </tr>
    <tr>
      <th>z</th>
      <td>[zero]</td>
    </tr>
  </tbody>
</table>
</div>




```python
# Join the dataframes
df\
.join(_df, on="classe").drop_column("classe")\
.join(_df_initial, on="initial")\
.display()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>filename</th>
      <th>initial</th>
      <th>classe</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE/wow/919...</td>
      <td>w</td>
      <td>[wow]</td>
    </tr>
    <tr>
      <th>1</th>
      <td>/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE/wow/6a2...</td>
      <td>w</td>
      <td>[wow]</td>
    </tr>
    <tr>
      <th>2</th>
      <td>/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE/wow/682...</td>
      <td>w</td>
      <td>[wow]</td>
    </tr>
    <tr>
      <th>3</th>
      <td>/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE/wow/beb...</td>
      <td>w</td>
      <td>[wow]</td>
    </tr>
    <tr>
      <th>4</th>
      <td>/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE/wow/d37...</td>
      <td>w</td>
      <td>[wow]</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>13</th>
      <td>/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE/left/6a...</td>
      <td>l</td>
      <td>[left]</td>
    </tr>
    <tr>
      <th>14</th>
      <td>/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE/left/e3...</td>
      <td>l</td>
      <td>[left]</td>
    </tr>
    <tr>
      <th>15</th>
      <td>/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE/left/68...</td>
      <td>l</td>
      <td>[left]</td>
    </tr>
    <tr>
      <th>16</th>
      <td>/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE/left/e7...</td>
      <td>l</td>
      <td>[left]</td>
    </tr>
    <tr>
      <th>17</th>
      <td>/tmp/1y4gwaS7LjYUhwTex1-lNHJJ71nLEh3fE/left/65...</td>
      <td>l</td>
      <td>[left]</td>
    </tr>
  </tbody>
</table>
<p>150 rows × 3 columns</p>
</div>



## Ressources
* Vanilla:  https://en.wikipedia.org/wiki/Vanilla_software
* Sandbox: https://en.wikipedia.org/wiki/Sandbox_(software_development)
* All you need is docker: https://www.theregister.com/2014/05/23/google_containerization_two_billion/
* Dev in containers : https://code.visualstudio.com/docs/remote/containers
* Delta lake partitions: https://k21academy.com/microsoft-azure/data-engineer/delta-lake/
