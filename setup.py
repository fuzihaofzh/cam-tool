#!/usr/bin/env python
from setuptools import setup, find_packages

setup(
    name='cam-tool',
    version='0.0.5',
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
          'fire', 'pyyaml', 'redis', 'tabulate'
    ],
    entry_points={
          'console_scripts': [
              'cam = cam.cam:main'
          ]
    },
)