import time
import unittest
from decimal import Decimal
from unittest.mock import Mock, patch

from dynamodb_timeseries import dynamodb
from dynamodb_timeseries.dynamodb import TABLE_DEFINITION


class TestDynamoDB(unittest.TestCase):
    def test_encode_value(self):
      self.assertEqual(Decimal(3.5), dynamodb.encode_value(3.5))
      self.assertEqual('testing', dynamodb.encode_value('testing'))

    def test_decode_value(self):
      self.assertEqual('testing', dynamodb.decode_value('testing'))
      self.assertEqual(3.5, dynamodb.decode_value(Decimal(3.5)))

    def test_decode_item(self):
      item = {'tag': 'testing', 'timestamp': 123456789, 'value': Decimal(42.42)}
      self.assertEqual([123456789, 42.42], dynamodb.decode_item(item))

    def test__create_table(self):
      client = Mock()
      dynamodb._create_table('testing', client)
      client.create_table.assert_called()
    
    def test_wait_for_table_to_exist(self):
      client = Mock()
      client.get_waiter.return_value = waiter = Mock()
      dynamodb.wait_for_table_to_exist('testing', client)
      client.get_waiter.assert_called()
      waiter.wait.assert_called_with(TableName='testing', WaiterConfig={'Delay': 2, 'MaxAttempts': 30})
    
    @patch('dynamodb_timeseries.dynamodb.CLIENT')
    @patch('dynamodb_timeseries.dynamodb.DDB_RESOURCE')
    def test_query(self, resource, client):
      end_ts = int(time.time() * 1000)
      start_ts = end_ts - 3600000
      resource.Table.return_value = table = Mock()
      table.query.return_value = {
        'Items': [{'tag': 'tag', 'timestamp': start_ts, 'value': 42}]
      }
      self.assertEqual([[start_ts, 42]], dynamodb.query('testing', 'tag', start_ts, end_ts))
      table.query.assert_called()
    
    @patch('dynamodb_timeseries.dynamodb.CLIENT')
    @patch('dynamodb_timeseries.dynamodb.DDB_RESOURCE')
    @patch('boto3.client')
    def test_create_table(self, boto3_client, resource, client):
      client.meta.region_name = 'us-west-2'
      boto3_client.return_value = east_client = Mock()
      dynamodb.create_table('testing', ['us-west-2', 'us-east-1'])
      client.create_table.assert_called_with(**dict(TABLE_DEFINITION, **{'TableName': 'testing'}))
      boto3_client.assert_called_with('dynamodb', region_name='us-east-1')
      east_client.create_table.assert_called_with(**dict(TABLE_DEFINITION, **{'TableName': 'testing'}))
      client.create_global_table.assert_called_with(GlobalTableName='testing', ReplicationGroup=[{'RegionName': 'us-east-1'}, {'RegionName': 'us-west-2'}])
    
    @patch('dynamodb_timeseries.dynamodb.CLIENT')
    @patch('dynamodb_timeseries.dynamodb.DDB_RESOURCE')
    def test_put(self, resource, client):
      timestamp = int(time.time() * 1000)
      resource.Table.return_value = table = Mock()
      dynamodb.put('testing', 'tag', timestamp, 'value')
      resource.Table.assert_called_with('testing')
      table.put_item.assert_called_with(Item={'tag': 'tag', 'timestamp': timestamp, 'value': 'value'})
      


if __name__ == '__main__':
    unittest.main()
