from datetime import datetime
from dateutil.rrule import rrule, YEARLY, MONTHLY, WEEKLY, DAILY, HOURLY

from dynamodb_timeseries.exceptions import InvalidIntervalException
class TableResolver:
    def __init__(self, prefix, interval: int = MONTHLY):
        if interval not in [YEARLY, MONTHLY, WEEKLY, DAILY, HOURLY]:
            raise InvalidIntervalException('Interval must be in [YEARLY, MONTHLY, WEEKLY, DAILY, HOURLY]')
        self.__prefix = prefix
        self.__interval = interval

    def __get_date_string(self, d: datetime) -> str:
        if self.__interval == YEARLY:
            return d.strftime('%Y')
        elif self.__interval == MONTHLY:
            return d.strftime('%Y-%m')
        elif self.__interval == WEEKLY:
            return d.strftime('%Y-week-%U')
        elif self.__interval == DAILY:
            return d.strftime('%Y-%m-%d')
        elif self.__interval == HOURLY:
            return d.strftime('%Y-%m-%d-hour-%H')

    @property
    def interval(self):
        return self.__interval

    @property
    def prefix(self):
        return self.__prefix

    def get_table(self, timestamp_ms: int) -> str:
        d = datetime.utcfromtimestamp(timestamp_ms / 1000)
        return f'{self.__prefix}-{self.__get_date_string(d)}'

    def get_tables(self, start_timestamp_ms: int, end_timestamp_ms: int) -> [str]:
        start_date = datetime.utcfromtimestamp(start_timestamp_ms / 1000)
        end_date = datetime.utcfromtimestamp(end_timestamp_ms / 1000)
        dates = rrule(self.__interval, dtstart=start_date, until=end_date)
        tables = [f'{self.__prefix}-{self.__get_date_string(d)}' for d in dates]
        last_table = self.get_table(end_timestamp_ms)
        if last_table not in tables:
            tables.append(last_table)
        return tables
