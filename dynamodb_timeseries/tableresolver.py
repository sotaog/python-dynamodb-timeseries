from datetime import date
from dateutil.rrule import rrule, YEARLY, MONTHLY, WEEKLY, DAILY, HOURLY


class TableResolver:
    def __init__(self, prefix, interval: int = MONTHLY):
        if interval not in [YEARLY, MONTHLY, WEEKLY, DAILY, HOURLY]:
            raise Exception('Interval must be in [YEARLY, MONTHLY, WEEKLY, DAILY, HOURLY]')
        self.prefix = prefix
        self.interval = interval

    def __get_date_string(self, d: date) -> str:
        if self.interval == YEARLY:
            return d.strftime('%Y')
        elif self.interval == MONTHLY:
            return d.strftime('%Y-%m')
        elif self.interval == WEEKLY:
            return d.strftime('%Y-week-%U')
        elif self.interval == DAILY:
            return d.strftime('%Y-%m-%d')
        elif self.interval == HOURLY:
            return d.strftime('%Y-%m-%d-%H')

    def get_table(self, timestamp_ms: int) -> str:
        d = date.fromtimestamp(timestamp_ms / 1000)
        return f'{self.prefix}-{self.__get_date_string(d)}'

    def get_tables(self, start_timestamp_ms: int, end_timestamp_ms: int) -> [str]:
        start_date = date.fromtimestamp(start_timestamp_ms / 1000)
        end_date = date.fromtimestamp(end_timestamp_ms / 1000)
        dates = rrule(self.interval, dtstart=start_date, until=end_date)
        return list(set([f'{self.prefix}-{self.__get_date_string(d)}' for d in dates] + [self.get_table(end_timestamp_ms)]))

if __name__ == '__main__':
    import time
    import pprint
    tr = TableResolver('timeseries-testing', interval=MONTHLY)
    pprint.pprint(tr.get_table(time.time()))
    start_ts = time.time()
    end_ts = start_ts + (86400 * 100)
    pprint.pprint(tr.get_tables(start_ts, end_ts))