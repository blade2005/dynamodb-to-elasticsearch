# DynamoDB to ElasticSearch

This package allow you to easily ZIP a Lambda function and start processing DynamoDB Streams in order to index your DynamoDB objects in ElasticSearch.

It processed the following events:

   - INSERT
   - REMOVE
   - MODIFY

We force an index refresh for each event, for close to realtime indexing.

The DynamoDB JSON objects are unmarshaled and types are correctly converted. (Binary types have never been tested though)

List of numbers and strings are converted to list of Strings. ES doesn't allow arrays of different data types.

Blog reference explaining what we're doing here: https://aws.amazon.com/blogs/compute/indexing-amazon-dynamodb-content-with-amazon-elasticsearch-service-using-aws-lambda/

Make file now supports changing upload destination with ```FUNCNAME```

Default is DynamotoES

```
make FUNCNAME=ZDDynamotoES
```
