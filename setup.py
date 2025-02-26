#!/usr/bin/env python3

__author__ = "xi"

from setuptools import setup

if __name__ == '__main__':
    with open('README.md') as file:
        long_description = file.read()
    setup(
        name='liblogging',
        packages=[
            'liblogging',
        ],
        version='0.1.6',
        description='Utilities for logging and sending logs.',
        long_description_content_type='text/markdown',
        long_description=long_description,
        license='Apache-2.0 license',
        author='xi',
        author_email='gylv@mail.ustc.edu.cn',
        url='https://github.com/XoriieInpottn/liblogging',
        platforms='any',
        classifiers=[
            'Programming Language :: Python :: 3',
        ],
        include_package_data=True,
        zip_safe=True,
        install_requires=[]
    )
