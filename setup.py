"""
to install:

    python setup.py install
"""
from setuptools import setup


setup(
    name="arbiter",
    description="A task-dependency solver",
    long_description=open('README.rst').read(),
    version="0.4.0",
    author="Brendan Curran-Johnson",
    author_email="brendan.curran.johnson@invenia.ca",
    license="MIT License",
    url="https://github.com/invenia/Arbiter",

    packages=(
        "arbiter",
    ),

    install_requires=(
        'futures',
    ),

    tests_require=(
        'asyncio',
        'coverage',
        'nose',
        'python-coveralls',
    ),

    classifiers=(
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy",
    ),
)
