import unittest

from dynamodb_timeseries import TableResolver, YEARLY, MONTHLY, WEEKLY, DAILY, HOURLY


class TestTableResolver(unittest.TestCase):
    def test_init(self):
        tr = TableResolver('testing-only', interval=MONTHLY)
        self.assertEqual(tr.prefix, 'testing-only')
        self.assertEqual(tr.interval, MONTHLY)

    def test_invalid_interval(self):
        with self.assertRaises(Exception) as context:
            _ = TableResolver('testing-only', interval=999)
        self.assertTrue('Interval must be in' in str(context.exception))

    def test_get_tables(self):
        """
        Test that multiple tables resolve correctly
        """
        tr = TableResolver('testing-only', interval=YEARLY)
        self.assertEqual(tr.get_tables(1577836799999, 1577836800000), ['testing-only-2019', 'testing-only-2020'])

    def test_yearly_get_table(self):
        """
        Test that yearly tables resolve correctly
        """
        tr = TableResolver('testing-only', interval=YEARLY)
        self.assertEqual(tr.get_table(1577836799999), 'testing-only-2019')
        self.assertEqual(tr.get_table(1577836800000), 'testing-only-2020')

    def test_monthly_get_table(self):
        """
        Test that monthly tables resolve correctly
        """
        tr = TableResolver('testing-only', interval=MONTHLY)
        self.assertEqual(tr.get_table(1577836799999), 'testing-only-2019-12')
        self.assertEqual(tr.get_table(1577836800000), 'testing-only-2020-01')

    def test_weekly_get_table(self):
        """
        Test that weekly tables resolve correctly
        """
        tr = TableResolver('testing-only', interval=WEEKLY)
        self.assertEqual(tr.get_table(1577836799999), 'testing-only-2019-week-52')
        self.assertEqual(tr.get_table(1577836800000), 'testing-only-2020-week-00')

    def test_daily_get_table(self):
        """
        Test that daily tables resolve correctly
        """
        tr = TableResolver('testing-only', interval=DAILY)
        self.assertEqual(tr.get_table(1577836799999), 'testing-only-2019-12-31')
        self.assertEqual(tr.get_table(1577836800000), 'testing-only-2020-01-01')

    def test_hourly_get_table(self):
        """
        Test that hourly tables resolve correctly
        """
        tr = TableResolver('testing-only', interval=HOURLY)
        self.assertEqual(tr.get_table(1577836799999), 'testing-only-2019-12-31-hour-23')
        self.assertEqual(tr.get_table(1577836800000), 'testing-only-2020-01-01-hour-00')


if __name__ == '__main__':
    unittest.main()
