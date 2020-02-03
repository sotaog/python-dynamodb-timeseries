# DynamoDB TimeSeries

[![Build Status](https://api.travis-ci.com/sotaog/python-dynamodb-timeseries.svg?branch=master)](https://travis-ci.com/sotaog/python-dynamodb-timeseries)

A set of libraries that make it easier to use DynamoDB as a timeseries datastore.

# Installation
You can install DynamoDB TimeSeries from PyPI:

```
pip install python-dynamodb-timeseries
```

DynamoDB TimeSeries is supported on Python 3.6 and above.

## Table Resolver

Helps partition your data into tables by hour, day, month, or year.

## Timeseries

Put datapoints singly or in batches, query the tags in parallel across tags and assemble the results. Uses Table Resolver to partition data.

## Datapoints

Datapoints are "narrow", meaning a datapoint consists of a tag, timestamp (ms since the epoch), and value. These are stored as individual items in DynamoDB.

## Examples

```python
from dynamodb_timeseries import MONTHLY, Timeseries

table_name_prefix = 'timeseries-testing'
timeseries = Timeseries(table_name_prefix, interval=MONTHLY, regions=['us-west-2', 'us-east-2'])
tag = 'example-tag'
timestamp = 1555722540000
value = 123.45
timeseries.put(tag, timestamp, value)
timeseries.put_batch([[tag, timestamp, value]])
datapoints = timeseries.query(['example-tag'])
```

## Environment Variables

`DYNAMODB_TIMESERIES_REGIONS` Regions to create tables in. You can set this rather than passing the argument to each instance of TimeSeries.