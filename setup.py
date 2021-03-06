#!/usr/bin/env python

from setuptools import setup

setup(
    setup_requires=['pbr>=1.9', 'setuptools>=17.1', 'pytest-runner'],
    tests_require=['pytest'],
    python_requires='>=3.6',
    pbr=True,
)
