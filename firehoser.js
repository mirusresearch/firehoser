'use strict';

var _ = require('lodash');
var AWS = require('aws-sdk');
var async = require('async');
var JaySchema = require('jayschema');
var moment = require('moment');

var schemaValidator = new JaySchema();

class DeliveryStream{
    constructor(name, awsConfig=null, schema=null, retryInterval=1500, firehose=null, logger=null){
        this.maxIngestion = 400;
        this.maxDrains = 3;
        this.maxRetries = 40;
        this.name = name;
        if (awsConfig !== null){
            AWS.config.update(awsConfig);
        }
        this.schema = schema;
        this.retryInterval = retryInterval;
        this.firehose = firehose ? firehose : new AWS.Firehose({params: {DeliveryStreamName: name}});
        this.log = logger ? logger : () => {};
    }

    validateRecord(record){
        return schemaValidator.validate(record, this.schema);
    }

    validateRecords(records){
        if (!this.schema){
            return [records, []];
        }
        let validRecords = [];
        let invalidRecords = [];
        _.forEach(records, (record) => {
            let validationErrors = this.validateRecord(record);
            if (_.isEmpty(validationErrors)){
                validRecords.push(record);
            } else {
                invalidRecords.push({
                    type: "schema",
                    originalRecord: record,
                    description: validationErrors[0].desc,
                    details: validationErrors[0],
                });
            }
        });
        return [validRecords, invalidRecords];
    }

    formatRecord(record){
        return {Data: record + '\n'};
    }

    putRecord(record){
        return this.putRecords([record]);
    }

    putRecords(records){
        this.log(`DeliveryStream.putRecords() called with ${records.length} records.`);
        return new Promise((resolve, reject) => {
            // Validate records against a schema, if necessary.
            let [validRecords, invalidRecords] = this.validateRecords(records);

            // Split the records into reasonably-sized chunks.
            records = _.map(validRecords, this.formatRecord);
            let chunks = _.chunk(records, this.maxIngestion);
            let tasks = [];
            for (let i=0; i < chunks.length; i++){
                tasks.push(this.drain.bind(this, chunks[i]));
            }

            // Schedule the chunks all at the same time.
            this.log(`Kicking off ${tasks.length} calls to drain() for ${records.length} records.`);
            async.parallelLimit(tasks, this.maxDrains, function(err, results){
                let allErrors = invalidRecords.concat(_.flatten(results));
                if (err || !_.isEmpty(allErrors)){
                    return reject(err, allErrors);
                }
                return resolve();
            });
        });
    }

    drain(records, cb, numRetries=0){
        var leftovers = [];
        this.log(`Draining ${records.length} records.  Pass #${numRetries + 1}`);
        this.firehose.putRecordBatch({Records: records}, function(firehoseErr, resp){
            // Stuff broke!
            if (firehoseErr){
                return cb(firehoseErr);
            }

            // Not all records make it in, but firehose keeps on chugging!
            if (resp.FailedPutCount > 0){
            }

            // Push errored records back into the next list.
            for (let [orig, result] of _.zip(records, resp.RequestResponses)){
                if (!_.isUndefined(result.ErrorCode)){
                    this.log(`Got ErrorCode ${result.ErrorCode} for record ${orig}`);
                    leftovers.push({
                        type: "firehose",
                        description: result.ErrorMessage,
                        details: {
                            ErrorCode: result.ErrorCode,
                            ErrorMessage: result.ErrorMessage,
                        },
                        originalRecord: orig,
                    });
                }
            }

            // Recurse!
            if (leftovers.length && numRetries < this.maxRetries){
                // We're about to recurse, let the child handle storing error details.
                leftovers = _.map(leftovers, (leftover) => { return _.pick(leftover, ['originalRecord'])})
                return setTimeout(function(){
                    this.drain.bind(this, leftovers, cb, numRetries + 1);
                }, this.retryInterval);
            } else {
                return cb(null, leftovers);
            }
        });
    }
}

class JSONDeliveryStream extends DeliveryStream {
    formatRecord(record){
        return super.formatRecord(JSON.stringify(record));
    }
}

class QueuableDeliveryStream extends DeliveryStream {
    constructor(name, maxTime=30000, maxSize=500, ...args){
        super(name, ...args);
        this.queue = [];
        this.timeout = null;
        this.maxTime = maxTime;
        this.maxSize = maxSize;
        this.promise = null;
        setInterval(this.drainQueue.bind(this), this.maxTime);
    }

    putRecords(records){
        this.log(`QueuableDeliveryStream.putRecords() called with ${records.length} records.`);
        this.queue.push(...records);
        if (this.promise === null){
            this.promise = new Promise((resolve, reject) => {
                this.resolver = resolve;
                this.rejecter = reject;
            });
        }
        this.log(`queue size is: ${this.queue.length}, maxSize is: ${this.maxSize}.`);
        if (this.queue.length >= this.maxSize){
            // Queue's full!
            this.log(`queue is full, draining immediately.`);
            setImmediate(this.drainQueue.bind(this));
        }
        return this.promise;
    }

    drainQueue(){
        this.log(`Countdown timer expired or queue limit reached.`);
        this.log(`Time to drain the queue of ${this.queue.length} records.`);
        let toQueue = this.queue.splice(0, this.queue.length);
        if (!toQueue.length){
            this.log(`No records in queue, not draining anything.`);
            return;
        }
        super.putRecords(toQueue).then(this.resolver, this.rejecter).then(() => {
            this.promise = null;
            this.rejecter = null;
            this.resolver = null;
        });
    }
}

class QueuableJSONDeliveryStream extends QueuableDeliveryStream {
    formatRecord(record){
        return super.formatRecord(JSON.stringify(record));
    }
}

function makeRedshiftTimestamp(input){
    return moment(input).utc().format('YYYY-MM-DD HH:mm:ss')
}

module.exports = {
    DeliveryStream: DeliveryStream,
    JSONDeliveryStream: JSONDeliveryStream,
    QueuableDeliveryStream: QueuableDeliveryStream,
    QueuableJSONDeliveryStream: QueuableJSONDeliveryStream,
    makeRedshiftTimestamp: makeRedshiftTimestamp
};
