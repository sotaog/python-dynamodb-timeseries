import pathlib
from setuptools import setup

# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.md").read_text()

setup(
    name='python-dynamodb-timeseries',
    version='0.2.2',
    description='Library for working with timeseries data on dynamodb',
    long_description=README,
    long_description_content_type="text/markdown",
    url='https://github.com/sotaog/python-dynamodb-timeseries',
    author='Zachary Fox',
    author_email='zachreligious@gmail.com',
    license='MIT',
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    packages=['dynamodb_timeseries'],
    install_requires=['boto3', 'python-dateutil']
)
