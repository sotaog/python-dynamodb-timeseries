from datetime import datetime
from dateutil.rrule import rrule, YEARLY, MONTHLY, WEEKLY, DAILY, HOURLY


class TableResolver:
    def __init__(self, prefix, interval: int = MONTHLY):
        if interval not in [YEARLY, MONTHLY, WEEKLY, DAILY, HOURLY]:
            raise Exception('Interval must be in [YEARLY, MONTHLY, WEEKLY, DAILY, HOURLY]')
        self.prefix = prefix
        self.interval = interval

    def __get_date_string(self, d: datetime) -> str:
        if self.interval == YEARLY:
            return d.strftime('%Y')
        elif self.interval == MONTHLY:
            return d.strftime('%Y-%m')
        elif self.interval == WEEKLY:
            return d.strftime('%Y-week-%U')
        elif self.interval == DAILY:
            return d.strftime('%Y-%m-%d')
        elif self.interval == HOURLY:
            return d.strftime('%Y-%m-%d-hour-%H')

    def get_table(self, timestamp_ms: int) -> str:
        d = datetime.utcfromtimestamp(timestamp_ms / 1000)
        return f'{self.prefix}-{self.__get_date_string(d)}'

    def get_tables(self, start_timestamp_ms: int, end_timestamp_ms: int) -> [str]:
        start_date = datetime.utcfromtimestamp(start_timestamp_ms / 1000)
        end_date = datetime.utcfromtimestamp(end_timestamp_ms / 1000)
        dates = rrule(self.interval, dtstart=start_date, until=end_date)
        tables = [f'{self.prefix}-{self.__get_date_string(d)}' for d in dates]
        last_table = self.get_table(end_timestamp_ms)
        if last_table not in tables:
            tables.append(last_table)
        return tables
