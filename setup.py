#!/usr/bin/env python

from distutils.core import setup

setup(
    name="pipy",
    version="0.0.1",
    description="Package for interactive transactional ETL pipelines in Jupyter Lab",
    author="Ronald Smits",
    author_email="rhsmits@me.com",
    packages=["pipy"],
    install_requires=["graphviz", "testfixtures"],
)
