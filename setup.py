from setuptools import find_packages
from setuptools import setup

setup(
    name="graft",
    version="0.0.1",
    description="An unopinionated asynchronous DAG DSL and executor built into Python.",
    keywords="async graph io",
    author="Cerek Hillen",
    author_email="cerekh@gmail.com",
    url="https://github.com/crockeo/graft",
    packages=find_packages(),
    install_requires=[
        "gevent>=20, <21",
    ],
)
