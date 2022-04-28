#!/usr/bin/env python3
from pprint import pprint
from typing import List
import argparse
import boto3


class DynamoGsiMigrator(object):

    def __init__(self, environment: str, region: str, local: bool):
        if local:
            dynamodb = boto3.resource('dynamodb', region, endpoint_url='http://localhost:8000')
        else:
            dynamodb = boto3.resource('dynamodb', region)

        self.table = dynamodb.Table(f'{environment}-config-ord')
        self.scan_filter_and_conditional = {
            'ScanFilter': {
                'pk': {
                    'AttributeValueList': [
                        'mt:cfg-svc:kv'
                    ],
                    'ComparisonOperator': 'BEGINS_WITH'
                },
                'gsipk3': {
                    'ComparisonOperator': 'NULL'  # NULL in DynamoDB == NOT EXISTS in SQL
                },
                'gsisk3': {
                    'ComparisonOperator': 'NULL'
                }
            },
            'ConditionalOperator': 'AND'
        }

    # Scan table and count items where pk starts with "mt:cfg-svc:kv"
    def _count_items(self):
        print(f'Scan table: {self.table.name} and count items where pk starts with "mt:cfg-svc:kv"')
        response = self.table.scan(
            Select='COUNT',
            **self.scan_filter_and_conditional
        )
        pprint(response)
        self.scan_count = response["Count"]
        print(f'Items Count: {self.scan_count} \n')

    # Scan table and Return items and pk, sk attributes only
    def _get_items(self) -> List[dict]:
        print(f'Scan table: {self.table.name} and get items where pk starts with "mt:cfg-svc:kv"')
        scan_args = {
            **self.scan_filter_and_conditional,
            'AttributesToGet': ['pk', 'sk'],
            'Limit': 5,  # Remove it, only for testing
            'Select': 'SPECIFIC_ATTRIBUTES',
        }
        response = self.table.scan(**scan_args)
        pprint(response)
        items = response['Items']

        while response.get('LastEvaluatedKey'):
            print('\n There are more results, fetching! \n')
            response = self.table.scan(**scan_args, ExclusiveStartKey=response['LastEvaluatedKey'])
            pprint(response)
            items.extend(response['Items'])

        self.items_count = len(items)
        print(f'\n Total items to be updated: {self.items_count} \n')
        return items

    # Update filtered items
    def _update_items(self, items: List[dict]):
        print('Updating filtered items')
        update_responses = []
        failed_update_responses = []
        for item in items:
            gsipk3 = item['pk'][0: 13]          # Rename to gsipk2
            rest = item['pk'].split(":")[3]
            gsisk3 = rest                       # Rename to gsisk2
            rest_array = rest.split('+')
            if len(rest_array) > 2:
                gsisk3 = rest_array[0] + '+' + rest_array[1]

            update_response = self.table.update_item(
                Key={
                    'pk': item['pk'],
                    'sk': item['sk']
                },
                UpdateExpression="SET gsipk3 = :pk_value, gsisk3 = :sk_value",  # Rename to gsipk2, gsisk2
                ExpressionAttributeValues={
                    ':pk_value': gsipk3,
                    ':sk_value': gsisk3
                },
                ReturnValues="UPDATED_NEW"
            )
            if update_response['ResponseMetadata']['HTTPStatusCode'] != 200:
                failed_update_responses.append(update_response)
            update_responses.append(update_response['Attributes'])

        failed_updates_count = len(failed_update_responses)
        successful_updates_count = len(update_responses) - failed_updates_count
        print(f'Successful updates: {successful_updates_count}')
        print(f'Failed updates: {len(failed_update_responses)}')
        if failed_updates_count > 0:
            print('Failed updates: ')
            pprint(failed_update_responses, sort_dicts=False)
        return update_responses

    def _compare_count_and_updated(self):
        if self.scan_count == self.items_count:
            print(f'Initial scan count returned: {self.scan_count} items matching the update criteria')
            print(f'Items scan returned: {self.items_count} items to be updated')
            print(f'There are more items to be updated. Please run the script again')

    def migrate(self):
        self._count_items()
        items = self._get_items()
        self._update_items(items)
        self._compare_count_and_updated()


if __name__ == '__main__':
    argparser = argparse.ArgumentParser(description="Add and populate gsipk3 and gsisk3 attributes")
    argparser.add_argument("--environment", required=False, default='dev', help="PMT environment")
    argparser.add_argument("--region", required=False, default='us-east-1', help="PMT region")
    argparser.add_argument("--local", required=False, default=True, help="local dynamoDB")
    args = argparser.parse_args()

    migrator = DynamoGsiMigrator(args.environment, args.region, args.local)
    migrator.migrate()
