<h1 align="center">
  <br>
  NMesh
  <br>
  <br>
  <img src="https://drive.google.com/uc?id=1adCikDmjjDULmn-3R7_wpyyJX-nzKlHb">
</h1>

<p align="center">
  <a href="#modules">Modules</a> •
  <a href="#code-structure">Code structure</a> •
  <a href="#installing-the-application">Installing the application</a> •
  <a href="#makefile-commands">Makefile commands</a> •
  <a href="#environments">Environments</a> •
  <a href="#running-the-application">Running the application</a>
  <a href="#ressources">Ressources</a>
</p>


NMesh is a Python package that provides two high-level features:
- A simple Mesh processor
- A list of tool to convert mesh files into point cloud

You can reuse your favorite Python packages such as NumPy, SciPy and Cython to extend ZakuroCache integration.


# Modules

At a granular level, NMesh is a library that consists of the following components:

| Component | Description |
| ---- | --- |
| **nmesh** | Contains the implementation of NMesh |
| **nmesh.core** | Contain the functions executed by the library. |
| **nmesh.pc** | Processor for the point cloud|
| **nmesh.tests** | Unit tests |



# Code structure
```python
from setuptools import setup
from nmesh import __version__
setup(
    name='nmesh',
    version=__version__,
    packages=[
        "nmesh",
        "nmesh.core",
        "nmesh.pc",
        "nmesh.tests"
    ],
    url='https://github.com/JeanMaximilienCadic/nmesh',
    include_package_data=True,
    package_data={"": ["*.yml"]},
    long_description="".join(open("README.md", "r").readlines()),
    long_description_content_type='text/markdown',
    license='MIT',
    author='Jean Maximilien Cadic',
    python_requires='>=3.6',
    install_requires=[r.rsplit()[0] for r in open("requirements.txt")],
    author_email='support@cadic.jp',
    description='GNU Tools for python',
    classifiers=[
        "Programming Language :: Python :: 3.6",
        "License :: OSI Approved :: MIT License",
    ]
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

Install the package:
```bash
# Clone this repository and install the code
git clone https://github.com/JeanMaximilienCadic/nmesh

# Go into the repository
cd nmesh
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
```
from nmesh import NMesh, cfg
m = NMesh(cfg.drive.bull)
m.show()
```

## Ressources
* Vanilla:  https://en.wikipedia.org/wiki/Vanilla_software
* Sandbox: https://en.wikipedia.org/wiki/Sandbox_(software_development)
* All you need is docker: https://www.theregister.com/2014/05/23/google_containerization_two_billion/
* Dev in containers : https://code.visualstudio.com/docs/remote/containers
* Delta lake partitions: https://k21academy.com/microsoft-azure/data-engineer/delta-lake/




