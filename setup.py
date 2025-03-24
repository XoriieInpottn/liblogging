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
            'liblogging.sending',
        ],
        entry_points={
            'console_scripts': [
                'liblogging_collector = liblogging.sending.log_collector:main'
            ]
        },
        version='0.1.13',
        description='Utilities for logging and sending logs.',
        long_description_content_type='text/markdown',
        long_description=long_description,
        license='Apache-2.0 license',
        author='xi',
        author_email='gylv@mail.ustc.edu.cn, huangfuyb@163.com',
        url='https://github.com/XoriieInpottn/liblogging',
        platforms='any',
        classifiers=[
            'Programming Language :: Python :: 3',
        ],
        include_package_data=True,
        zip_safe=True,
        install_requires=[],
        extras_require={
            'collector': ['kafka-python==2.0.2']
        }
    )
