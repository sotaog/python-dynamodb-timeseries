from setuptools import setup

setup(
    name='python-dynamodb-timeseries',
    version='0.1.0',
    description='Library for working with timeseries data on dynamodb',
    url='',
    author='Zachary Fox',
    author_email='zachreligious@gmail.com',
    license='MIT',
    packages=['dynamodb_timeseries'],
    install_requires=['boto3', 'python-dateutil']
)
