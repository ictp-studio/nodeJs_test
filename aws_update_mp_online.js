#!/usr/bin/env node
var mountName = process.argv[2];

// Load the AWS SDK for Node.js
var AWS = require('aws-sdk');
// Set the region 
AWS.config.update({region: 'us-east-1'});

//var tableName = "minaret-dev";
var tableName = "broadcastChannel_dev";

const dynamoDB = new AWS.DynamoDB.DocumentClient()
dynamoDB
  .update({
    TableName: tableName,
    Key: {
        //"mount_name": "\"/azaanAbuBakr.opus\"",
        //"mount_name": JSON.stringify(mountName),
        "mountName": mountName,
    },
    UpdateExpression: 'set current_status = :status',
    ExpressionAttributeValues: {
      ":status": "Online",
    },
  })
  .promise()
  .then(data => console.log(data.Attributes))
  .catch(console.error)