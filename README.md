# graft

An unopinionated asynchronous DAG DSL and executor built into Python.

## Overview

Oftentimes, Python is not the core infrastructure powering large systems. At a certain point, Python buckles under its
own weight; its runtime was simply not designed to allow for the control necessary for certain levels of load. Instead,
if Python is used, it is used as a glue that binds together other components of the system. This shift signifies a
switch from a CPU-bound workload to an IO-bound workload.

There are myriad existing approaches to extracting more performance out of IO-bound workloads in Python, like the [built
in asyncio](https://docs.python.org/3/library/asyncio.html), [gevent](https://gevent.org), and others. In general, these
approaches have one of two problems:

* They require that you aggressively buy in to their specific async system

* They provide insufficient structure, allowing programmers to create an inefficient program

`graft` attempts to address these issues _for request/response_-style programs by:

* Structuring the API to allow for gradual rewrites; graphs are recursive structures where subgraphs are first class
  objects. Parts of the program can be rewritten as a self-contained graph and subsequently added to a larger graph as a
  migration continues.

* Aggressively decoupling code from its execution order; many of the inefficiencies from IO-bound systems come from
  overhead encouraged by a procedural language. In practice programmers are often trying to describe a system of
  dependent IO operations. By offloading thework of scheduling an executing these operations, programmers can increase
  the parallelism of their IO waits.

## Installation

**This package is not on pip, sorry :(. I'll get around to it eventually.**

```sh
$ git clone https://github.com/crockeo/graft
$ cd graft
$ pip install .
```

## Contributing

We'll have more here soon, but:

* probably use `virtualenv`! this is written on Python 3.8 and you might not want to install that on your system
  directly

* please install [pre-commit](https://pre-commit.com/), and do `pre-commit install` in the repo directory

## License

[MIT Open Source](/LICENSE)
