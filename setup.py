#!/usr/bin/env python
# -*- coding: utf8 -*-

# Import required modules
import re
from distutils.core import setup
import py2exe

__author__ = 'Victoria Morris'
__license__ = 'MIT License'
__version__ = '1.0.0'
__status__ = '4 - Beta Development'

# Version
version = '1.0.0'

# Long description
long_description = ''

# List requirements.
# All other requirements should all be contained in the standard library
requirements = [
    'py2exe',
    'regex',
    'pyperclip',
]

# Setup
setup(
    console=[
        'bin/identities_graph.py',
    ],
    zipfile=None,
    options={
        'py2exe': {
            'bundle_files': 0,
        }
    },
    name='identities_tools',
    version=version,
    author='Victoria Morris',
    url='',
    license='MIT',
    description='Tools for reconciling names and identities.',
    long_description=long_description,
    packages=['identities_tools'],
    scripts=[
        'bin/identities_graph.py',
    ],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python'
    ],
    requires=requirements
)
