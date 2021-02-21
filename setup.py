import os
import pathlib

import setuptools
from pkg_resources import parse_requirements
from setuptools import setup


dir_path = os.path.dirname(os.path.realpath(__file__))
file_path = os.path.join(dir_path, 'requirements.txt')


with pathlib.Path(file_path).open() as requirements_txt:
    reqs = [
        str(requirement)
        for requirement
        in parse_requirements(requirements_txt)
    ]

setup(
    name='dbt-model-tests',
    version='0.0.1',
    packages=setuptools.find_packages(),
    install_requires=reqs,
    include_packaged_data=True,
)
