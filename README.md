
<h1 align="center">
  <br>
  <a href="https://drive.google.com/uc?id=1IadQpNk2653aaflkXL-EBxJDWX43l0KP"><img src="https://drive.google.com/uc?id=1IadQpNk2653aaflkXL-EBxJDWX43l0KP" alt="IDGraph" width="200"></a>
  <br>
    bpd
  <br>
</h1>

<p align="center">
  <a href="#code-structure">Code</a> •
  <a href="#how-to-use">How To Use</a> •
  <a href="#docker">Docker</a> •
  <a href="#PythonEnv">PythonEnv</a> •
  <a href="#Databricks">Databricks</a> •

[![Build Status](https://dev.azure.com/catalinamarketing/jp-placeholder/_apis/build/status/cmj-bpd?branchName=master)](https://dev.azure.com/catalinamarketing/jp-placeholder/_build/latest?definitionId=812&branchName=master)

## Code design
* We recommend using Docker for dev and production. Therefore we encourage its usage all over the repo.
* We have a `vanilla` and `sandbox` environment. 
  * `Vanilla` refers to a prebuilt docker image that already contains system dependencies.
  * `Sandbox` refers a prebuilt docker image that contains the code of this repo.
* Semantic versioning https://semver.org/ . We commit fix to `a.b.x`, features to `a.x.c` and stable release (master) to `x.b.c`. 
* PR are done to `dev` and reviewed for additional features. This should only be reviewed by the engineers in the team.
* PR are done to `master` for official (internal) release of the codes. This should be reviewed by the maximum number of engineers.   
* The ETL jobs are scatter accross sequential refinement of the data `landing/bronze/silver/gold` 
* Modules and scripts: Any piece of code that can of use for parent module modules should be moved at a higher level. 
  * eg: `functional.py (1)`  contains common funtions for `etl.bronze` and `etl.silver` where `functional.py (2)` contains common functions ofr `etl.bronze` only.
```
...
├── etl
│   ├── bronze
│   │   ├── __init__.py
│   │   └── __main__.py
│   ├── functional.py (2)
│   ├── __init__.py
│   └── landing
│       ├── __init__.py
│       └── __main__.py
├── functional.py (1)
├── __init__.py
...
```
* Modules should ideally contain a `__main__.py` that demo an exeution of the module
  * `etl/bronze/__main__.py` describes an etl job for the creation of the bronze partition
  * `trainer/__main__.py` describes the training pipeline



### Motivation
https://www.thedataincubator.com/blog/2018/05/23/sqlite-vs-pandas-performance-benchmarks/

# Code structure
```python
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
    url="https://github.com/catalina-jp/cmj-bpd",
    license="CMJ",
    author="CADIC Jean-Maximilien",
    python_requires=">=3.6",
    install_requires=[r.rsplit()[0] for r in open("requirements.txt")],
    author_email="cjean@catmktg.com",
    description="bpd",
    platforms="linux_debian_10_x86_64",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: CMJ License",
    ],
)

```


# How to use
To clone and run this application, you'll need [Git](https://git-scm.com) and [ https://docs.docker.com/docker-for-mac/install/]( https://docs.docker.com/docker-for-mac/install/) and Python installed on your computer. 
From your command line:

Install the cmj-bpd:
```bash
# Clone this repository and install the code
git clone git@ssh.dev.azure.com:v3/catalinamarketing/jp-placeholder/cmj-bpd

# Go into the repository
cd cmj-red
```

# Makefile
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
clean
build_wheels
auto_branch 
databricks_replace
```
### PythonEnv (not recommended)
```
make install_wheels
``` 

### Docker (recommended)
```shell
make build
make docker_run_master
```

## Ressources
* Vanilla:  https://en.wikipedia.org/wiki/Vanilla_software
* Sandbox: https://en.wikipedia.org/wiki/Sandbox_(software_development)
* All you need is docker: https://www.theregister.com/2014/05/23/google_containerization_two_billion/
* Dev in containers : https://code.visualstudio.com/docs/remote/containers
* Delta lake partitions: https://k21academy.com/microsoft-azure/data-engineer/delta-lake/