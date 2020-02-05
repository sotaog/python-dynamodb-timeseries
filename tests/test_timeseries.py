import time
import unittest
from unittest.mock import Mock, patch

from dynamodb_timeseries import TimeSeries, DEFAULT_REGIONS, MONTHLY, HOURLY


class TestTimeseries(unittest.TestCase):
    @patch('dynamodb_timeseries.dynamodb.CLIENT')
    def test_init(self, _client):
      ts = TimeSeries('testing')
      self.assertEqual(ts.table_name_prefix, 'testing')
      self.assertEqual(ts.regions, DEFAULT_REGIONS)
      self.assertEqual(ts.tr.interval, MONTHLY)

    @patch('dynamodb_timeseries.dynamodb.CLIENT')
    def test_non_default_init(self, _client):
      ts = TimeSeries('testing', interval=HOURLY, regions=['us-east-1'])
      self.assertEqual(ts.table_name_prefix, 'testing')
      self.assertEqual(ts.regions, ['us-east-1'])
      self.assertEqual(ts.tr.interval, HOURLY)
    
    @patch('dynamodb_timeseries.dynamodb.CLIENT')
    @patch('dynamodb_timeseries.dynamodb.DDB_RESOURCE')
    def test_query(self, resource, client):
      resource.Table.return_value = table = Mock()
      client.list_tables.return_value = {'TableNames': ['testing-1970-01']}
      table.query = Mock()
      table.query.return_value = {'Items': []}
      ts = TimeSeries('testing')
      end_timestamp_ms = 12345
      start_timestamp_ms = 0
      resp = ts.query(['tag'], start_timestamp_ms=start_timestamp_ms, end_timestamp_ms=end_timestamp_ms, limit=1, order='desc')
      self.assertEqual(resp, {'tag': []})


if __name__ == '__main__':
    unittest.main()
