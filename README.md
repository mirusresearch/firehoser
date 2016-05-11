# firehoser
A friendly wrapper around AWS Kinesis Firehose with retry logic and custom queuing behavior.  Get your data from node into S3 or Redshift in as easy a manner as possible.

## Installation
`npm install firehoser`

## Usage
To use Firehoser, you need to create an instance of a DeliveryStream.  There are several variations, but they all expose the same API: `putRecord` and it's pluralized counterpart `putRecords`.

The basic `DeliveryStream` only requires a single argument, which is the name of the delivery stream:

```node
var AWS = require('aws-sdk');
var firehoser = require('firehoser')

AWS.config.update({
  accessKeyId: 'hardcoded-credentials',
  secretAccessKey: 'are-not-a-good-idea'
});

let firehose = new firehoser.DeliveryStream('my_delivery_stream_name');

// Send a single record to Kinesis Firehose...
firehose.putRecord('value1|value2|value3');
```

`putRecords` behaves identically to `putRecord` except that the former accepts an Array of records.

```node
// Send multiple records...
firehose.putRecords([
  'value1|value2|value3',
  'valueX|valueY|valueZ',
]);

```

Both `putRecord` and `putRecords` return a Promise which resolve once the data has been successfully accepted by AWS.  If the rate at which you call them exceeds the capacity of the delivery stream to which you are sending them, they will continue to retry the send until they succeed, and only then will the promise resolve.

Amazon Kinesis Firehose limits the number of records you can send at a single time to 500.  Firehoser automatically chunks your records into batches of 400 to stay well below this limit.

A common use case for Firehose is to send JSON data into Redshift (or even just S3).  This is easily accomplished by using a `JSONDeliveryStream`:

```node
let firehose = new firehoser.JSONDeliveryStream('my_delivery_stream_name');

firehose.putRecord({
  jsonIs: 'a cool format',
  itMakes: 'pipe-separated values look so dated.'
});

firehose.putRecords([
  {a: 1},
  {a: 2},
  {a: 3}
]).then(()=>{
  console.log("Cool!");
});
```

Both `DeliveryStream` and `JSONDeliveryStream` will immediately send data to AWS Kinesis Firehose as soon as (and as often as) you call `putRecord`.  Sometimes what you want instead is to queue up records as they are generated, and only send them to AWS Kinesis Firehose once a certain number of them have been gathered, or a certain time limit has passed.  For these cases, we've created `QueuableDeliveryStream` and `QueuableJSONDeliveryStream`.  Simply pass in a cutoff value for the number of records, or the staleness of records that you're comfortable with, and we'll wait until one of those limits is passed before sending off the data.

```node
let maxDelay = 15000; // milliseconds
let maxQueued = 100;
let firehose = new firehoser.QueueableJSONDeliveryStream(
  'my_delivery_stream_name',
  maxDelay,
  maxQueued
);

firehose.putRecord({
 id: 'rec1'
}).then(()=>{
  console.log('rec1 sent to AWS!');
});

firehose.putRecord({
  id: 'rec2'
}).then(()=>{
  console.log('rec2 sent to AWS!');
});

//  ... wait ~15 seconds and see both console messages fire at the same time!
```

#### Misc ####
*Take off, ya hoser!*
