const moment = require('moment');
const AWS = require("aws-sdk");
const ddb = new AWS.DynamoDB.DocumentClient();

exports.timelineHandler = async (event) => {
    var ticker = event["queryStringParameters"]['ticker'];
    let minTime = moment(event["queryStringParameters"]['min_time']);
    let maxTime = moment(event["queryStringParameters"]['max_time']);

    console.log('request ticker timeline', ticker, 'min time', minTime, 'max time', maxTime);

    try {
        const promises = new Array();

        // query timeline stats
        promises.push(new Promise(async (resolve, reject) => {
            var timeline = new Array();

            var params = {
                TableName: "daily-stats",
                KeyConditionExpression: "ticker = :t and #ts between :min and :max",
                ExpressionAttributeNames: {
                    "#ts": "timestamp"
                },
                ExpressionAttributeValues: {
                    ":t": ticker,
                    ":min": minTime.unix() * 1000,
                    ":max": maxTime.unix() * 1000
                }
            };

            try {
                var result = await ddb.query(params).promise();
                timeline.push(...result.Items);

                while ('LastEvaluatedKey' in result) {
                    params['LastEvaluatedKey'] = result['LastEvaluatedKey'];
                    result = await ddb.query(params).promise();
                    timeline.push(...result.Items);
                }

                resolve(timeline);
            } catch (e) {
                reject(e);
            }
        }));

        // query news
        promises.push(new Promise(async (resolve, reject) => {
            try {
                var params = {
                    TableName: "news",
                    KeyConditionExpression: "ticker = :t and #ts between :min and :max",
                    ExpressionAttributeNames: {
                        "#ts": "timestamp"
                    },
                    Limit: 10,
                    ExpressionAttributeValues: {
                        ":t": ticker,
                        ":min": minTime.unix() * 1000,
                        ":max": maxTime.unix() * 1000
                    }
                }

                var result = await ddb.query(params).promise();
                resolve(result.Items)
            } catch (e) {
                reject(e)
            }
        }))

        // query latest tweets
        promises.push(new Promise(async (resolve, reject) => {
            try {
                var params = {
                    TableName: "tweets-new",
                    KeyConditionExpression: "ticker = :t and #ts between :min and :max",
                    ExpressionAttributeNames: {
                        "#ts": "timestamp"
                    },
                    Limit: 10,
                    ExpressionAttributeValues: {
                        ":t": ticker,
                        ":min": minTime.unix() * 1000,
                        ":max": maxTime.unix() * 1000
                    },
                    ScanIndexForward: false
                }

                var result = await ddb.query(params).promise();
                resolve(result.Items)
            } catch (e) {
                reject(e)
            }
        }))

        const [timeline, news, tweets] = await Promise.all(promises);

        return {
            headers: {
                'Access-Control-Allow-Origin': '*'
            },
            statusCode: 200,
            body: JSON.stringify({
                timeline: timeline,
                news: news,
                tweets: tweets
            })
        };
    } catch (e) {
        console.log('get brand timeline failed', e);
        return {
            headers: {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Request-Method': 'POST, GET, OPTIONS, DELETE, OPTION, PUT',
                'Access-Control-Allow-Headers': 'Content-Type, Authorization',
                'Access-Control-Allow-Credentials': 'true',
            },
            statusCode: 500,
            body: JSON.stringify({
                err: e.message
            })
        };
    }
};
