import logging
import os
import sys
import time
from decimal import Decimal
from multiprocessing import Pool
from typing import List, Union

from dynamodb_timeseries import exceptions
from dynamodb_timeseries.dynamodb import CLIENT, create_table, list_tables, put, put_batch, query
from dynamodb_timeseries.tableresolver import MONTHLY, TableResolver

logger = logging.getLogger(__name__)


def _latest(table_names, tag):
    end_ts = int(time.time() * 1000)
    for table_name in table_names:
        results = query(table_name, tag, 0, end_ts, limit=1, order='desc')
        if results:
            return results
    return []


class TimeSeries:
    MAX_CONCURRENCY = 100

    @staticmethod
    def __get_regions():
        regions = os.getenv('DYNAMODB_TIMESERIES_REGIONS')
        if regions:
            return [r for r in regions.split(',')]
        return [CLIENT.meta.region_name]

    def __init__(self, table_name_prefix: str, interval: int = MONTHLY, regions: List[str] = []):
        self.table_name_prefix = table_name_prefix
        self.__tables = list_tables(table_name_prefix)
        self.tr = TableResolver(table_name_prefix, interval=interval)
        if not regions:
            regions = self.__get_regions()
        self.regions = regions

    def __create_table(self, table_name: str):
        create_table(table_name, replicated_regions=self.regions)
        self.__tables.append(table_name)
        self.__tables.sort()

    @property
    def tables(self):
        return self.__tables

    def query(self, tags: List[str], start_timestamp_ms: int, end_timestamp_ms: int, limit: int = 0, order: Union['asc', 'desc'] = 'desc'):
        table_names = self.tr.get_tables(start_timestamp_ms, end_timestamp_ms)
        table_names = [name for name in table_names if name in self.__tables]
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

    def latest(self, tags: List[str]):
        table_names = self.__tables
        table_names.sort(reverse=True)
        results = {}
        pool = Pool(processes=min(self.MAX_CONCURRENCY, len(tags)))
        for tag in tags:
            results[tag] = pool.apply_async(_latest, (table_names, tag, ))
        resp = {}
        for tag, result in results.items():
            resp[tag] = result.get()
        return resp

    def put(self, tag: str, timestamp_ms: int, value: any):
        table_name = self.tr.get_table(timestamp_ms)
        try:
            return put(table_name, tag, timestamp_ms, value)
        except exceptions.TableDoesNotExistException:
            # Table does not exist
            self.__create_table(table_name)
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
            if table_name not in self.__tables:
                self.__create_table(table_name)
            put_batch(table_name, records)
