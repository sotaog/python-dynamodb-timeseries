import logging
from decimal import Decimal
from typing import List, Union

import boto3
from boto3.dynamodb.conditions import Attr, Key

logger = logging.getLogger(__name__)
DDB_RESOURCE = boto3.resource('dynamodb')
CLIENT = boto3.client('dynamodb')

def __create_table(table_name: str, client):
    logger.info(f'Creating table {table_name} in {client.meta.region_name}')
    kwargs = {
        'TableName': table_name,
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
    try:
        client.create_table(**kwargs)
    except client.exceptions.ResourceInUseException:
        # Table is being created
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
    __create_table(table_name, CLIENT)
    if replicated_regions:
        for region_name in replica_regions:
            c = boto3.client('dynamodb', region_name=region_name)
            __create_table(table_name, c)
        CLIENT.create_global_table(
            GlobalTableName=table_name,
            ReplicationGroup=[{'RegionName': r} for r in all_regions]
        )


def query(table_name: str, tag: str, start_timestamp_ms: int, end_timestamp_ms: int, limit: int = 0, order: Union['asc', 'desc'] = 'desc') -> List[dict]:
    logger.info(f'DynamoDB Timeseries Query: {table_name} {tag} {start_timestamp_ms} {end_timestamp_ms} {limit} {order}')
    table = DDB_RESOURCE.Table(table_name)
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
        # Table does not exist, asking for something out of range
        return []
    data.extend([decode_item(i) for i in resp['Items']])
    while 'LastEvaluatedKey' in resp:
        if limit and len(data) >= limit:
            break
        kwargs['ExclusiveStartKey'] = resp['LastEvaluatedKey']
        resp = table.query(**kwargs)
        data.extend([decode_item(i) for i in resp['Items']])
    return data


if __name__ == '__main__':
    import logging
    import sys
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    table_name = 'testing-timeseries-2020-01'

    def delete():
        for region in ['us-west-2', 'us-east-2']:
            c = boto3.client('dynamodb', region_name=region)
            c.delete_table(TableName=table_name)
            waiter = c.get_waiter('table_not_exists')
            waiter.wait(
                TableName=table_name,
                WaiterConfig={
                    'Delay': 2,
                    'MaxAttempts': 30
                }
            )
    # delete()
    create_table(table_name, replicated_regions=['us-west-2', 'us-east-2'])
