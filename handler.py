import json
import os
import uuid
from datetime import datetime

import botocore
import boto3
import requests

locations = None


def user_create(event, context):
    user_table = boto3.resource('dynamodb').Table(os.environ['USER_TABLE'])
    body = json.loads(event['body'])
    user_table.put_item(
        Item={
            'id': body['id'],
            'name': body['name']
        }
    )
    return {
        'statusCode': 200,
        'body': json.dumps({'result': 'ok'})
    }


def wallet_charge(event, context):
    user_table = boto3.resource('dynamodb').Table(os.environ['USER_TABLE'])
    history_table = boto3.resource('dynamodb').Table(os.environ['PAYMENT_HISTORY_TABLE'])
    sqs = boto3.client('sqs')
    body = json.loads(event['body'])
    response = user_table.update_item(
        Key={
            'id': body['userId']
        },
        AttributeUpdates={
            'amount': {
                'Value': body['chargeAmount'],
                'Action': 'ADD'
            }
        },
        UpdateExpression='ADD amount :chargeAmount',
        ExpressionAttributeValues={
            ':chargeAmount': body['chargeAmount']
        },
        ReturnValues='ALL_NEW'
    )
    history_table.put_item(
        Item={
            'userId': body['userId'],
            'transactionId': body['transactionId'],
            'chargeAmount': body['chargeAmount'],
            'locationId': body['locationId'],
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    )

    sqs.send_message(
        QueueUrl=os.environ['NOTIFICATION_QUEUE'],
        MessageBody=json.dumps({
        'transactionId': body['transactionId'],
        'userId': body['userId'],
        'chargeAmount': body['chargeAmount'],
        'totalAmount': int(response['Attributes']['amount'])
    }))

    return {
        'statusCode': 202,
        'body': json.dumps({'result': 'Assepted. Please wait for the notification.'})
    }


def wallet_use(event, context):
    user_table = boto3.resource('dynamodb').Table(os.environ['USER_TABLE'])
    history_table = boto3.resource('dynamodb').Table(os.environ['PAYMENT_HISTORY_TABLE'])
    sqs = boto3.client('sqs')
    body = json.loads(event['body'])
    try:
        response = user_table.update_item(
            Key={
                'id': body['userId']
            },
            UpdateExpression='ADD amount :useAmount',
            ExpressionAttributeValues={
                ':useAmount': body['useAmount'] * -1
            },
            ConditionExpression=boto3.dynamodb.conditions.Attr('amount').gte(body['useAmount']),
            ReturnValues='ALL_NEW'
        )
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            return {
                'statusCode': 400,
                'body': json.dumps({'errorMessage': 'There was not enough money.'})
            }
    history_table.put_item(
        Item={
            'userId': body['userId'],
            'transactionId': body['transactionId'],
            'useAmount': body['useAmount'],
            'locationId': body['locationId'],
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    )

    sqs.send_message(
        QueueUrl=os.environ['NOTIFICATION_QUEUE'],
        MessageBody=json.dumps({
        'transactionId': body['transactionId'],
        'userId': body['userId'],
        'useAmount': body['useAmount'],
        'totalAmount': int(response['Attributes']['amount'])
    }))

    return {
        'statusCode': 202,
        'body': json.dumps({'result': 'Assepted. Please wait for the notification.'})
    }


def wallet_transfer(event, context):
    user_table = boto3.resource('dynamodb').Table(os.environ['USER_TABLE'])
    history_table = boto3.resource('dynamodb').Table(os.environ['PAYMENT_HISTORY_TABLE'])
    sqs = boto3.client('sqs')
    body = json.loads(event['body'])

    try:
        from_result = user_table.update_item(
            Key={
                'id': body['fromUserId']
            },
            UpdateExpression='ADD amount :transferAmount',
            ExpressionAttributeValues={
                ':transferAmount': body['transferAmount'] * -1
            },
            ConditionExpression=boto3.dynamodb.conditions.Attr('amount').gte(body['transferAmount']),
            ReturnValues='ALL_NEW'
        )
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            return {
                'statusCode': 400,
                'body': json.dumps({'errorMessage': 'There was not enough money.'})
            }
    to_result = user_table.update_item(
        Key={
            'id': body['toUserId']
        },
        UpdateExpression='ADD amount :transferAmount',
        ExpressionAttributeValues={
            ':transferAmount': body['transferAmount']
        },
        ConditionExpression=boto3.dynamodb.conditions.Attr('amount').gte(body['transferAmount']),
        ReturnValues='ALL_NEW'
    )
    history_table.put_item(
        Item={
            'userId': body['fromUserId'],
            'transactionId': body['transactionId'],
            'useAmount': body['transferAmount'],
            'locationId': body['locationId'],
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    )
    history_table.put_item(
        Item={
            'userId': body['toUserId'],
            'transactionId': body['transactionId'],
            'chargeAmount': body['transferAmount'],
            'locationId': body['locationId'],
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    )

    sqs.send_message(
        QueueUrl=os.environ['NOTIFICATION_QUEUE'],
        MessageBody=json.dumps({
        'transactionId': body['transactionId'],
        'userId': body['fromUserId'],
        'useAmount': body['transferAmount'],
        'totalAmount': int(from_result['Attributes']['amount']),
        'transferTo': body['toUserId']
    }))

    sqs.send_message(
        QueueUrl=os.environ['NOTIFICATION_QUEUE'],
        MessageBody=json.dumps({
        'transactionId': body['transactionId'],
        'userId': body['toUserId'],
        'chargeAmount': body['transferAmount'],
        'totalAmount': int(to_result['Attributes']['amount']),
        'transferFrom': body['fromUserId']
    }))

    return {
        'statusCode': 202,
        'body': json.dumps({'result': 'Assepted. Please wait for the notification.'})
    }


def get_user_summary(event, context):
    user_table = boto3.resource('dynamodb').Table(os.environ['USER_TABLE'])
    history_table = boto3.resource('dynamodb').Table(os.environ['PAYMENT_HISTORY_TABLE'])
    params = event['pathParameters']
    user = user_table.get_item(
        Key={'id': params['userId']}
    )
    payment_history = history_table.query(
        KeyConditions={
            'userId': {
                'AttributeValueList': [
                    params['userId']
                ],
                'ComparisonOperator': 'EQ'
            }
        }
    )
    sum_charge = 0
    sum_payment = 0
    times_per_location = {}
    for item in payment_history['Items']:
        sum_charge += item.get('chargeAmount', 0)
        sum_payment += item.get('useAmount', 0)
        location_name = _get_location_name(item['locationId'])
        if location_name not in times_per_location:
            times_per_location[location_name] = 1
        else:
            times_per_location[location_name] += 1
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'userName': user['Item']['name'],
            'currentAmount': int(user['Item']['amount']),
            'totalChargeAmount': int(sum_charge),
            'totalUseAmount': int(sum_payment),
            'timesPerLocation': times_per_location
        })
    }


def get_payment_history(event, context):
    history_table = boto3.resource('dynamodb').Table(os.environ['PAYMENT_HISTORY_TABLE'])
    params = event['pathParameters']
    payment_history_result = history_table.query(
        KeyConditions={
            'userId': {
                'AttributeValueList': [
                    params['userId']
                ],
                'ComparisonOperator': 'EQ'
            }
        },
        IndexName='timestampIndex',
        ScanIndexForward=False
    )

    payment_history = []
    for p in payment_history_result['Items']:
        if 'chargeAmount' in p:
            p['chargeAmount'] = int(p['chargeAmount'])
        if 'useAmount' in p:
            p['useAmount'] = int(p['useAmount'])
        p['locationName'] = _get_location_name(p['locationId'])
        del p['locationId']
        payment_history.append(p)

    return {
        'statusCode': 200,
        'body': json.dumps(payment_history)
    }


def send_notification(event, context):
    for record in event['Records']:
        requests.post(os.environ['NOTIFICATION_ENDPOINT'], json=json.loads(record['body']))


def _get_location_name(location_id):
    global locations
    if locations is None:
        locations = json.loads(open('location.json', 'r').read())
    return locations[str(location_id)]
