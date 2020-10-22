import multiprocessing
from setuptools import setup, find_packages
import os
import glob

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name = "Covid-19",
    version = "0.1",
    packages = find_packages(),

    # Dependencies on other packages:
    # Couldn't get numpy install to work without
    # an out-of-band: sudo apt-get install python-dev
    setup_requires   = ['pytest-runner'],
    install_requires = ['scikit-learn>=0.23.2',
                        'pandas>=1.1.1',
                        'xlrd>=1.2.0',           # Pandas Excel support
                        'openpyxl>=3.0.5',       # Pandas Excel support
                        'category-encoders>=2.2.2', # leave-one-out encoding
                        'seaborn>=0.11.0',
                        'dbf>=0.99.0'
                        ],

    #dependency_links = ['https://github.com/DmitryUlyanov/Multicore-TSNE/tarball/master#egg=package-1.0']
    # Unit tests; they are initiated via 'python setup.py test'
    #test_suite       = 'nose.collector',
    #test_suite       = 'tests',
    #test_suite        = 'unittest2.collector',
    tests_require    =['pytest',
                       'testfixtures>=6.14.1',
                       ],

    # metadata for upload to PyPI
    author = "Andreas Paepcke",
    author_email = "paepcke@cs.stanford.edu",
    description = "Predict voter turnout",
    long_description_content_type = "text/markdown",
    long_description = long_description,
    license = "BSD",
    keywords = "covid-19",
    url = "git@github.com:paepcke/covid19_voting.git",   # project home page, if any
)
