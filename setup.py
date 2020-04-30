#!/bin/env python3

import sys
import logging

from setuptools import setup, find_packages
from mysqlzfs.constants import MYSQLZFS_VERSION

install_requires = [
    "boto3",
    "psutil",
    "mysql-connector-python",
    "requests",
    "mysqlclient"
]


setup(
    name='mysqlzfs',
    version=MYSQLZFS_VERSION,
    packages=find_packages(),
    author="Jervin Real",
    author_email="github@pdb.dev",
    url='https://github.com/dotmanila/mysqlzfs',
    description='MySQL Backup Management via ZFS',
    long_description=open('README.md').read(),
    install_requires=install_requires,
    scripts=['mysqlzfs.py'],
    include_package_data=True,
    zip_safe=False,
    python_requires='>=3.5',
)