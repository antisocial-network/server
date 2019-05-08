const moment = require('moment');
const AWS = require("aws-sdk");
AWS.config.update({ region: 'eu-west-2', accessKeyId: '***REMOVED***', secretAccessKey: '***REMOVED***' });
const ddb = new AWS.DynamoDB.DocumentClient();

var tickers = ['@Apple', '@Nike', '@facebook', '@Nintendo', '@Tesco', '@Starbucks',
    '@LFC', '@warriors', '@Huawei', '@BAPEOFFICIAL', '@netflix', '@Tesla'];
// var tickers = ['@Apple', '@Nike'];

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
            // TODO rolling window
            // for (var i = 0; i < 23; i++) {

            // }
            var result = await ddb.query(params).promise();
            tweets.push(...result.Items);

            console.log('ticker', ticker, 'begin', begin.toString(), 'end', end.toString(), 'count', result.Items.length, "first scan")

            while ('LastEvaluatedKey' in result) {
                params['ExclusiveStartKey'] = result['LastEvaluatedKey'];
                result = await ddb.query(params).promise();
                tweets.push(...result.Items);

                console.log('ticker', ticker, 'begin', begin.toString(), 'end', end.toString(), 'count', result.Items.length, 'LastEvaluatedKey', result['LastEvaluatedKey'])
            }

            resolve({ 'tweets': tweets, 'date': begin });
        } catch (e) {
            reject(e);
        }
    });
}

function brandProcess(ticker, minDate, maxDate) {
    return new Promise(async (resolve, reject) => {
        var promises = new Array();

        do {
            let begin = moment(minDate);
            let end = moment(minDate.add(1, 'days'));

            console.log('request ticker', ticker, 'min time', minDate.toString(), 'max time', maxDate.toString());

            promises.push(dailyProcess(ticker, begin, end));
        } while (maxDate.isAfter(minDate));

        var timeline = new Array();

        try {
            const results = await Promise.all(promises);

            results.forEach(date => {
                console.log('result length for', date['date'].toString(), 'is', date['tweets'].length)

                var totalScore = 0;

                date['tweets'].forEach(tweet => {
                    totalScore += tweet['sentiment'];
                })

                timeline.push({
                    'ticker': ticker,
                    'average': totalScore / date['tweets'].length,
                    'date': date['date'],
                    "count": date['tweets'].length
                })
            })

            resolve(timeline);
        } catch (e) {
            reject(e);
        }
    })
}

promises = new Array();

tickers.forEach(ticker => {
    let minDate = moment('20190402');
    let maxDate = moment('20190506');

    // let minDate = moment('20190411');
    // let maxDate = moment('20190412');
    promises.push(brandProcess(ticker, minDate, maxDate));
});

Promise.all(promises).then(results => {
    results.forEach(async (timeline) => {
        console.log('timeline', timeline);

        var params = {
            RequestItems: {
                "daily-stats": []
            }
        }
        timeline.forEach(dailyStats => {
            dailyStats['timestamp'] = dailyStats['date'].unix() * 1000
            dailyStats['date'] = dailyStats['date'].toString()
            params.RequestItems['daily-stats'].push({
                PutRequest: {
                    Item: dailyStats
                }
            })
        });

        try {
            const response = await ddb.batchWrite(params).promise();
            console.log('writing timeline', timeline[0].ticker, 'response', response)
        } catch (e) {
            console.log('err writing timeline', timeline, e);
        }
    });
})
