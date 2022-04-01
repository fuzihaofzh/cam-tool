#!/usr/bin/env python
from setuptools import setup, find_packages

from pathlib import Path
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()
from cam.version import __version__

setup(
    name='cam-tool',
    version=__version__,
    description='Cloud Assignment Manager Tool',
    url='https://github.com/fuzihaofzh/cam-tool',
    author='',
    author_email='',
    license='',
    classifiers=[
        'Programming Language :: Python :: 3.7',
    ],
    keywords='Cloud Assignment Manager',
    packages=find_packages(),
    install_requires=[
          'fire', 'pyyaml', 'redis', 'tabulate', 'GPUtil', 'psutil'
    ],
    entry_points={
          'console_scripts': [
              'cam = cam.cam:main'
          ]
    },
    long_description=long_description,
    long_description_content_type='text/markdown',
    include_package_data=True
)