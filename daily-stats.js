const moment = require('moment');
const AWS = require("aws-sdk");
const stats = require("stats-lite")

AWS.config.update({ region: 'eu-west-2', accessKeyId: '***REMOVED***', secretAccessKey: '***REMOVED***' });
const ddb = new AWS.DynamoDB.DocumentClient();

function dailyProcess(ticker, begin, end) {
    return new Promise(async (resolve, reject) => {
        var tweets = new Array();

        var params = {
            TableName: "tweets-new",
            KeyConditionExpression: "ticker = :t and #ts between :min and :max",
            ExpressionAttributeNames: {
                "#ts": "timestamp"
            },
            ExpressionAttributeValues: {
                ":t": ticker,
                ":min": begin.unix() * 1000,
                ":max": end.unix() * 1000
            }
        };

        try {
            var result = await ddb.query(params).promise();
            tweets.push(...result.Items);

            console.log('ticker', ticker, 'begin', begin.toString(), 'end', end.toString(), 'count', result.Items.length, "first scan")

            while ('LastEvaluatedKey' in result) {
                params['ExclusiveStartKey'] = result['LastEvaluatedKey'];
                result = await ddb.query(params).promise();
                tweets.push(...result.Items);

                console.log('ticker', ticker, 'begin', begin.toString(), 'end', end.toString(), 'count', result.Items.length, 'LastEvaluatedKey', result['LastEvaluatedKey'])
            }

            if (tweets.length == 0) {
                return
            }

            var totalScore = 0;
            var sentiments = new Array();

            tweets.forEach(tweet => {
                totalScore += tweet['sentiment'];
                sentiments.push(tweet['sentiment']);
            })

            var params = {
                TableName: 'daily-stats',
                Item: {
                    'ticker': ticker,
                    'average': totalScore / tweets.length,
                    'date': begin.toString(),
                    "count": tweets.length,
                    'stdev': stats.stdev(sentiments),
                    'timestamp': begin.unix() * 1000
                }
            };

            const response = await ddb.put(params).promise();

            console.log('write daily stats', begin.toString(), params.Item, 'response', response)

            resolve();
        } catch (e) {
            reject(e);
        }
    });
}

async function brandProcess(ticker, minDate, maxDate) {
    var promises = new Array();

    do {
        let begin = moment(minDate);
        let end = moment(minDate.add(1, 'days'));

        console.log('request ticker', ticker, 'min time', minDate.toString(), 'max time', maxDate.toString());

        dailyProcess(ticker, begin, end);
    } while (maxDate.isAfter(minDate));

    try {
        await Promise.all(promises);
    } catch (e) {
        console.log('brand process failed', e)
    }
}

promises = new Array();

var ticker = process.argv[2];
let minDate = moment('20190403');
let maxDate = moment('20190506');

// let minDate = moment('20190411');
// let maxDate = moment('20190412');
brandProcess(ticker, minDate, maxDate).then(() => {
}).catch(e => {
    console.log('brand process failed', e)
});
