import os
from setuptools import setup, find_packages


# Utility function to read the README file.
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name="experimenter",
    version="0.1.0",
    author="Viktor Hangya",
    author_email="hangyav@gmail.com",
    description=(
        "A utility tool to help running experiments by defining how a given"
        "file/task should be produced and dependecies between tasks. It is"
        "similar to build tools, such as make, but more universal."
    ),
    license="GPLv3",
    url="https://github.com/hangyav/experimenter",
    packages=find_packages(include=['experimenter*']),
    long_description=read('README.md'),
    long_description_content_type="text/markdown",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: OS Independent",
    ],
    entry_points={
        'console_scripts': ['experimenter=experimenter.cli:main'],
    },
    install_requires=[
        'rpyc>=5.0.1',
        'dill>=0.3.3',
        'pyyaml>=6.0.0',
        'gputil>=1.4.0',
        'psutil>=5.7.2',
        'plumbum>=1.8.1',
    ],
    extras_require={
        'dev': [
        ]
    },
)
