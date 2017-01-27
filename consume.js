'use strict';

// Consumes messages from the SQS queue and forwards them as POST requests to the postUrl.

// The url of the SQS queue
var sqsUrl = 'https://sqs.us-east-1.amazonaws.com/518551549274/mobify-nostradamus';

// Your AWS access key id
var awsAccessKeyId = '<your AWS access key id>';

// Your AWS secret key
var awsSecretAccessKey = '<your AWS access secret key>';

// The url to forward events to
var postUrl = 'http://mobify-nostradamus.ngrok.io/event/';



var _ = require('lodash');
var async = require('async');
var request = require('supertest');

//http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/CloudWatch.html
var AWS = require('aws-sdk');
AWS.config.update({
                accessKeyId: awsAccessKeyId,
                secretAccessKey: awsSecretAccessKey,
                region: 'us-east-1'
            });
var sqs = new AWS.SQS();

/**
 * Gets the messages from SQS.
 */
function getNextSqsMessages(callback)
{
    var params = {
            QueueUrl: sqsUrl,
            MaxNumberOfMessages: 10,
            VisibilityTimeout: 1,
            WaitTimeSeconds: 0
        };

    sqs.receiveMessage(params, function(err, data)
    {
        if (err)
            return callback(err);

        if (data.Messages)
            callback(null, data.Messages);
        else
            getNextSqsMessages(callback)
    });
}

/**
 * Delete the messages from SQS.
 */
function deleteMsg(sqsMsg, callback)
{
    var params = {
        QueueUrl: sqsUrl,
        ReceiptHandle: sqsMsg.ReceiptHandle
    };
    sqs.deleteMessage(params, callback);
}

function forwardSqsMessages(recruse)
{
    function done(err)
    {
        if(err) console.trace(err);

        forwardSqsMessages(true);
    }

    getNextSqsMessages(function(err, sqsMsgs)
    {
        if(err) return done(err);

        async.each(
            sqsMsgs,
            function(msg, next)
            {
                async.parallel([
                    function(callback)
                    {
                        var body = JSON.parse(msg.Body);

                        if(body.type === 'event')
                        {
                            console.log('sending events:', body.events);
                            request(postUrl)
                                .post('')
                                .send(body.events)
                                .expect(200)
                                .end(callback);
                        }
                        else
                        {
                            callback();
                        }
                    },
                    //after we process a SQS message we have to delete it from SQS
                    async.apply(deleteMsg, msg)
                ], next);
            },
            done);
    });
}

forwardSqsMessages();
