# -*- coding: utf-8 -*-

# DO NOT EDIT THIS FILE!
# This file has been autogenerated by dephell <3
# https://github.com/dephell/dephell

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

readme = ''

setup(
    long_description=readme,
    name='etllib',
    version='0.1.4',
    description='Tools to simplify interfacing with bigquery and GCP',
    python_requires='==3.*,>=3.7.0',
    author='Matt Delaney',
    author_email='mcdelaney@gmail.com',
    packages=['etllib'],
    package_dir={"": "."},
    package_data={},
    install_requires=[
        'google-auth==1.*,>=1.20.1', 'google-cloud-bigquery==1.*,>=1.26.1',
        'google-cloud-bigquery-storage==1.*,>=1.0.0',
        'google-cloud-secret-manager==1.*,>=1.0.0',
        'google-cloud-storage==1.*,>=1.30.0', 'pandas==1.*,>=1.1.0',
        'pyarrow==1.*,>=1.0.0', 'slackclient==2.*,>=2.8.0',
        'sqlalchemy=="^1.3.19"'
    ],
    extras_require={"dev": ["pytest"]},
)