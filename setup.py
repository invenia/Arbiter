"""
to install:

    python setup.py install
"""
try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

DESCRIPTION = "A task-dependency solver"

try:
    LONG_DESCRIPTION = open('README.rst').read()
except:
    LONG_DESCRIPTION = DESCRIPTION

setup(
    name="arbiter",
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    version="0.3.0",
    author="Brendan Curran-Johnson",
    author_email="brendan@bcjbcj.ca",
    license="MIT License",
    url="https://github.com/invenia/Arbiter",

    packages=(
        "arbiter",
    ),

    tests_require=(
        'coverage',
        'nose',
        'python-coveralls',
    ),

    classifiers=(
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
    ),
)
