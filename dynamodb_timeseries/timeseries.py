import logging
import os
import sys
from decimal import Decimal
from multiprocessing import Pool
from typing import List, Union

from dynamodb_timeseries import exceptions, DEFAULT_REGIONS
from dynamodb_timeseries.dynamodb import create_table, put, put_batch, query
from dynamodb_timeseries.tableresolver import MONTHLY, TableResolver

logger = logging.getLogger(__name__)


class TimeSeries:
    MAX_CONCURRENCY = 100

    @staticmethod
    def __get_regions():
        regions = os.getenv('DYNAMODB_TIMESERIES_REGIONS')
        if regions:
            return [r for r in regions.split(',')]
        return DEFAULT_REGIONS

    def __init__(self, table_name_prefix: str, interval: int = MONTHLY, regions: List[str] = None):
        self.table_name_prefix = table_name_prefix
        self.tr = TableResolver(table_name_prefix, interval=interval)
        if regions is None:
            regions = self.__get_regions()
        self.regions = regions

    def query(self, tags: List[str], start_timestamp_ms: int, end_timestamp_ms: int, limit: int = 0, order: Union['asc', 'desc'] = 'desc'):
        table_names = self.tr.get_tables(start_timestamp_ms, end_timestamp_ms)
        if order == 'desc':
            table_names.sort(reverse=True)
        results = {}
        pool = Pool(processes=min(self.MAX_CONCURRENCY, len(table_names) * len(tags)))
        for table_name in table_names:
            for tag in tags:
                results[f'{table_name}:{tag}'] = pool.apply_async(
                    query,
                    (table_name, tag, start_timestamp_ms, end_timestamp_ms, limit, order, ))
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
            return put(table_name, tag, timestamp_ms, value)
        except exceptions.TableDoesNotExistException:
            # Table does not exist
            create_table(table_name, replicated_regions=self.regions)
            return put(table_name, tag, timestamp_ms, value)

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
            try:
                put_batch(table_name, records)
            except exceptions.TableDoesNotExistException:
                create_table(table_name, replicated_regions=self.regions)
                put_batch(table_name, records)
