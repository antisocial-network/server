const moment = require('moment');
const AWS = require("aws-sdk");
AWS.config.update({ region: 'eu-west-2', accessKeyId: '***REMOVED***', secretAccessKey: '***REMOVED***' });
const ddb = new AWS.DynamoDB.DocumentClient();

let minDate = moment('20190403');
let maxDate = moment('20190507');
// let minDate = moment('20190410');
// let maxDate = moment('20190411');
var ticker = process.argv[2];

async function rollingWindow() {
    do {
        minDate.add(1, 'days');

        var batchParams = {
            RequestItems: {
                "rolling-window-stats": []
            }
        };

        for (var i = 0; i < 23; i++) {
            var minTime = moment(minDate).add(i, 'hours');

            var tweets = new Array();

            var params = {
                TableName: "tweets-new",
                KeyConditionExpression: "ticker = :t and #ts between :min and :max",
                ExpressionAttributeNames: {
                    "#ts": "timestamp"
                },
                ExpressionAttributeValues: {
                    ":t": ticker,
                    ":min": minTime.unix() * 1000,
                    ":max": (minTime.unix() + 2 * 7200) * 1000
                }
            };

            try {
                var result = await ddb.query(params).promise();
                tweets.push(...result.Items);

                console.log('ticker', ticker, 'date', minDate.toString(), 'i', i, 'count', result.Items.length, "first scan")

                while ('LastEvaluatedKey' in result) {
                    params['ExclusiveStartKey'] = result['LastEvaluatedKey'];
                    result = await ddb.query(params).promise();
                    tweets.push(...result.Items);

                    console.log('ticker', ticker, 'date', minDate.toString(), 'i', i, 'count', result.Items.length, 'LastEvaluatedKey', result['LastEvaluatedKey'])
                }

                var totalScore = 0;

                tweets.forEach(tweet => {
                    totalScore += tweet['sentiment'];
                })

                const average = tweets.length == 0 ? 0 : totalScore / tweets.length;

                batchParams.RequestItems['rolling-window-stats'].push({
                    PutRequest: {
                        Item: {
                            'ticker': ticker,
                            'average': average,
                            'datestamp': minDate.unix() * 1000,
                            'date': minDate.toString(),
                            'timestamp': minTime.unix() * 1000,
                            "count": tweets.length
                        }
                    }
                })
            } catch (e) {
                console.log('scan failed', e)
            }
        }

        try {
            const response = await ddb.batchWrite(batchParams).promise();
            console.log('batch write rolling window stats', minDate.toString(), 'response', response)
        } catch (e) {
            console.log('batch write failed', e)
        }

        console.log('request ticker', ticker, 'min time', minDate.toString(), 'max time', maxDate.toString());
    } while (maxDate.isAfter(minDate));
}

rollingWindow().then(() => {

}).catch(e => {
    console.error('process rolling windows failed', e)
})