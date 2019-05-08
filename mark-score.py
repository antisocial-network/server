from sentiment import prediction

import boto3
import json
import sys
from decimal import Decimal

dynamodb = boto3.resource('dynamodb', region_name='eu-west-2', aws_access_key_id='***REMOVED***',
                          aws_secret_access_key='***REMOVED***')
table = dynamodb.Table('tweets')
newTable = dynamodb.Table('tweets-new')

track = ['@Apple', '@Nike', '@facebook', '@Nintendo', '@Tesco', '@Starbucks',
         '@LFC', '@warriors', '@Huawei', '@BAPEOFFICIAL', '@netflix', '@Tesla']


def process(response):
    tweets = []
    texts = []

    for tweet in response['Items']:
        tweet['timestamp'] = int(tweet['timestamp_ms'])
        del tweet['timestamp_ms']

        for brand in track:
            if brand in tweet['text']:
                tweet['ticker'] = brand
                tweets.append(tweet)
                break

    for tweet in tweets:
        texts.append(tweet['text'])

    sentiments = prediction(texts)

    for i in range(len(tweets)):
        tweets[i]['sentiment'] = Decimal(str(sentiments[i]))

    with newTable.batch_writer() as batch:
        for tweet in tweets:
            batch.put_item(
                Item=tweet
            )


if __name__ == '__main__':
    total = 0

    response = table.scan()

    total = total + len(response['Items'])

    while 'LastEvaluatedKey' in response:
        print(total, response['LastEvaluatedKey'], flush=True)
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        total = total + len(response['Items'])
        try:
            process(response)
        except:
            print("processing error:", sys.exc_info()[0])
            continue
