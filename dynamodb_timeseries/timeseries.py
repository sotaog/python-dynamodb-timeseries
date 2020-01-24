import logging
import sys
from decimal import Decimal
from multiprocessing import Pool
from typing import List, Union

import boto3
from boto3.dynamodb.conditions import Attr, Key

from dynamodb_timeseries.dynamodb import encode_value, decode_value, decode_item, create_table, wait_for_table_to_exist, query
from dynamodb_timeseries.tableresolver import MONTHLY, TableResolver

logger = logging.getLogger(__name__)


class TimeSeries:
    MAX_CONCURRENCY = 100

    def __init__(self, table_name_prefix: str, interval: int = MONTHLY, regions=['us-west-2', 'us-east-2']):
        self.table_name_prefix = table_name_prefix
        self.tr = TableResolver(table_name_prefix, interval=interval)
        self.client = boto3.client('dynamodb')
        self.ddb = boto3.resource('dynamodb')
        self.regions = regions

    def query(self, tags: List[str], start_timestamp_ms: int, end_timestamp_ms: int, limit: int = 0, order: Union['asc', 'desc'] = 'desc'):
        table_names = self.tr.get_tables(start_timestamp_ms, end_timestamp_ms)
        if order == 'desc':
            table_names.sort(reverse=True)
        results = {}
        pool = Pool(processes=self.MAX_CONCURRENCY)
        for table_name in table_names:
            for tag in tags:
                results[f'{table_name}:{tag}'] = pool.apply_async(
                    query, (table_name, tag, start_timestamp_ms, end_timestamp_ms, limit, order, ))
        pool.close()
        pool.join()
        resp = {}
        for key, result in results.items():
            table_name, tag = key.split(':', maxsplit=1)
            if tag not in resp:
                resp[tag] = []
            resp[tag].extend(result.get())
        if len(table_names) and limit:
            # Need to re-limit the results since you get limit * num tables
            for tag in resp:
                resp[tag] = resp[tag][0:limit]
        return resp

    def put(self, tag: str, timestamp_ms: int, value: any):
        table_name = self.tr.get_table(timestamp_ms)
        try:
            table = self.ddb.Table(table_name)
            return table.put_item(
                Item={
                    'tag': tag,
                    'timestamp': timestamp_ms,
                    'value': encode_value(value)
                }
            )
        except self.client.exceptions.ResourceNotFoundException:
            # Table does not exist
            create_table(table_name, replicated_regions=self.regions)
            return self.put(tag, timestamp_ms, value)
        except self.client.exceptions.ResourceInUseException:
            # Table is being created
            wait_for_table_to_exist(table_name, self.client)
            return self.put(tag, timestamp_ms, value)

    def __put_batch(self, table_name, records):
        logger.info(f'Putting {len(records)} records to {table_name}')
        try:
            table = self.ddb.Table(table_name)
            with table.batch_writer(overwrite_by_pkeys=['tag', 'timestamp']) as batch:
                for [tag, timestamp, value] in records:
                    item = {
                        'tag': tag,
                        'timestamp': timestamp,
                        'value': encode_value(value)
                    }
                    batch.put_item(Item=item)
        except self.client.exceptions.ResourceNotFoundException:
            # Table does not exist
            create_table(table_name, replicated_regions=self.regions)
            return self.__put_batch(table_name, records)
        except self.client.exceptions.ResourceInUseException:
            # Table is being created
            wait_for_table_to_exist(table_name, self.client)
            return self.__put_batch(table_name, records)

    def put_batch(self, records):
        # group by tables
        tables = {}
        for r in records:
            [_, ts, _] = r
            table_name = self.tr.get_table(ts)
            if table_name not in tables:
                tables[table_name] = []
            tables[table_name].append(r)
        for table_name, records in tables.items():
            self.__put_batch(table_name, records)
