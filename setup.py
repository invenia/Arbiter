"""
to install:

    python setup.py install
"""
from distutils.core import setup

setup(
    name="arbiter",
    version="0.2.0",
    author="Brendan Curran-Johnson",
    author_email="brendan.curran.johnson@invenia.ca",
    url="https://github.com/invenia/Arbiter",
    description="A concurrent task-runner that resolves dependency issues",
    long_description=open('README.rst', 'r').read(),
    packages=["arbiter"],
    license="LICENSE.txt",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy",
    ],
)
