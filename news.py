from newsapi import NewsApiClient
import datetime
import boto3
import time

track = {'@Apple': 'Apple', '@Nike': 'Nike', '@facebook': 'facebook', '@Nintendo': 'Nintendo', '@Tesco': 'Tesco', '@Starbucks': 'Starbucks',
         '@LFC': 'liverpool', '@warriors': 'warriors', '@Huawei': 'huawei', '@BAPEOFFICIAL': 'bape', '@netflix': 'netflix', '@Tesla': 'tesla'}

dynamodb = boto3.resource('dynamodb', region_name='eu-west-2', aws_access_key_id='***REMOVED***',
                          aws_secret_access_key='***REMOVED***')
table = dynamodb.Table('news')

# Init
newsapi = NewsApiClient(api_key='b2c22257fe3c49ae9d4df6329d77c268')


def strToTimestamp(date_str):
    dt_obj = datetime.datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ")
    return int(dt_obj.timestamp() * 1000)


for ticker, brand in list(track.items()):
    page = 1

    all_articles = newsapi.get_everything(q=brand,
                                          sources='bbc-news,the-verge',
                                          language='en',
                                          sort_by='relevancy',
                                          page_size=100)
    print(len(all_articles['articles']),
          'pieces of new of', brand, 'page', page)

    with table.batch_writer() as batch:
        for article in all_articles['articles']:
            article['ticker'] = ticker
            article['timestamp'] = strToTimestamp(article['publishedAt'])
            del article['publishedAt']
            batch.put_item(
                Item=article
            )
