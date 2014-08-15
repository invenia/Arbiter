"""
to install:

    python setup.py install
"""
from distutils.core import setup

setup(
    name="arbiter",
    version="0.1.0",
    author="Brendan Curran-Johnson",
    author_email="brendan.curran.johnson@invenia.ca",
    url="https://gitlab.invenia.ca/invenia/arbiter",
    description="A concurrent task-runner that resolves dependency issues",
    long_description=open('README.rst', 'r').read(),
    packages=["arbiter"],
    licens="LICENSE.txt",
)
