import logging
from decimal import Decimal
from typing import List, Union

import boto3
from boto3.dynamodb.conditions import Attr, Key

from dynamodb_timeseries import exceptions

logger = logging.getLogger(__name__)
DDB_RESOURCE = boto3.resource('dynamodb')
CLIENT = boto3.client('dynamodb')
TABLE_DEFINITION = {
    'AttributeDefinitions': [
        {
            'AttributeName': 'tag',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'timestamp',
            'AttributeType': 'N'
        },
    ],
    'KeySchema': [
        {
            'AttributeName': 'tag',
            'KeyType': 'HASH'
        },
        {
            'AttributeName': 'timestamp',
            'KeyType': 'RANGE'
        }
    ],
    'BillingMode': 'PAY_PER_REQUEST',
    'StreamSpecification': {
        'StreamEnabled': True,
        'StreamViewType': 'NEW_AND_OLD_IMAGES'
    },
}


def _create_table(table_name: str, client):
    logger.info(f'Creating table {table_name} in {client.meta.region_name}')
    kwargs = dict(TABLE_DEFINITION, **{'TableName': table_name})
    try:
        client.create_table(**kwargs)
    except client.exceptions.ResourceInUseException:
        # Table is being created (or being deleted but we'll just pretend that edge isn't there)
        pass
    wait_for_table_to_exist(table_name, client)


def encode_value(value):
    if isinstance(value, float):
        return Decimal(str(value))
    return value


def decode_value(value):
    if isinstance(value, Decimal):
        return float(value)
    return value


def decode_item(item):
    return [int(item['timestamp']), decode_value(item['value'])]


def wait_for_table_to_exist(table_name: str, client):
    waiter = client.get_waiter('table_exists')
    waiter.wait(
        TableName=table_name,
        WaiterConfig={
            'Delay': 2,
            'MaxAttempts': 30
        }
    )


def create_table(table_name: str, replicated_regions=[]):
    current_region = CLIENT.meta.region_name
    replica_regions = [r for r in replicated_regions if r != current_region]
    all_regions = replica_regions + [current_region]
    _create_table(table_name, CLIENT)
    if replica_regions:
        for region_name in replica_regions:
            c = boto3.client('dynamodb', region_name=region_name)
            _create_table(table_name, c)
        CLIENT.create_global_table(
            GlobalTableName=table_name,
            ReplicationGroup=[{'RegionName': r} for r in all_regions]
        )


def list_tables(table_name_prefix: str):
    table_names = []
    resp = CLIENT.list_tables()
    table_names.extend(resp['TableNames'])
    while 'LastEvaluatedTableName' in resp:
        resp = CLIENT.list_tables(ExclusiveStartTableName=resp['LastEvaluatedTableName'])
        table_names.extend(resp['TableNames'])
    return [name for name in table_names if name.startswith(table_name_prefix)]


def query(table_name: str, tag: str, start_timestamp_ms: int, end_timestamp_ms: int, limit: int = 0, order: Union['asc', 'desc'] = 'desc') -> List[dict]:
    logger.debug(
        f'DynamoDB Timeseries Query: {table_name} {tag} {start_timestamp_ms} {end_timestamp_ms} {limit} {order}')
    table = DDB_RESOURCE.Table(table_name)  # pylint: disable=no-member
    key_condition_expression = Key('tag').eq(tag) & Key('timestamp').between(
        Decimal(start_timestamp_ms), Decimal(end_timestamp_ms))
    kwargs = {
        'KeyConditionExpression': key_condition_expression,
        'ProjectionExpression': 'tag, #t, #v',
        'ExpressionAttributeNames': {'#t': 'timestamp', '#v': 'value'},
        'ScanIndexForward': (order == 'asc')
    }
    if limit:
        kwargs['Limit'] = limit
    data = []
    try:
        resp = table.query(**kwargs)
    except CLIENT.exceptions.ResourceNotFoundException:
        # Table does not exist, asking for something out of range of existing data
        return []
    data.extend([decode_item(i) for i in resp['Items']])
    while 'LastEvaluatedKey' in resp:
        if limit and len(data) >= limit:
            break
        kwargs['ExclusiveStartKey'] = resp['LastEvaluatedKey']
        resp = table.query(**kwargs)
        data.extend([decode_item(i) for i in resp['Items']])
    return data


def put(table_name: str, tag: str, timestamp_ms: int, value: any):
    try:
        table = DDB_RESOURCE.Table(table_name)  # pylint: disable=no-member
        return table.put_item(
            Item={
                'tag': tag,
                'timestamp': timestamp_ms,
                'value': encode_value(value)
            }
        )
    except CLIENT.exceptions.ResourceNotFoundException:
        raise exceptions.TableDoesNotExistException(f'{table_name} does not exist')
    except CLIENT.exceptions.ResourceInUseException:
        # Table is busy
        wait_for_table_to_exist(table_name, CLIENT)
        return put(table_name, tag, timestamp_ms, value)


def put_batch(table_name, records):
    logger.info(f'Putting {len(records)} records to {table_name}')
    try:
        table = DDB_RESOURCE.Table(table_name)  # pylint: disable=no-member
        with table.batch_writer(overwrite_by_pkeys=['tag', 'timestamp']) as batch:
            for [tag, timestamp, value] in records:
                item = {
                    'tag': tag,
                    'timestamp': timestamp,
                    'value': encode_value(value)
                }
                batch.put_item(Item=item)
    except CLIENT.exceptions.ResourceNotFoundException:
        raise exceptions.TableDoesNotExistException(f'{table_name} does not exist')
    except CLIENT.exceptions.ResourceInUseException:
        # Table is busy
        wait_for_table_to_exist(table_name, CLIENT)
        return put_batch(table_name, records)
