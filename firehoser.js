'use strict';

var _ = require('lodash');
var AWS = require('aws-sdk');
var async = require('async');
var JaySchema = require('jayschema');
var moment = require('moment');

var schemaValidator = new JaySchema();

class DeliveryStream{
    constructor(name, awsConfig=null, schema=null, retryInterval=1500, firehose=null){
        this.maxIngestion = 400;
        this.maxDrains = 3;
        this.name = name;
        if (awsConfig !== null){
            AWS.config.update(awsConfig);
        }
        this.schema = schema;
        this.retryInterval = retryInterval;
        this.firehose = firehose ? firehose : new AWS.Firehose({params: {DeliveryStreamName: name}});
    }

    validateRecord(record){
        if (this.schema === null){
            return;
        }
        let validationErrors = schemaValidator.validate(record, this.schema);
        if(!_.isEmpty(validationErrors)){
            let unwrapped = _.map(validationErrors,ve=>{return _.fromPairs(_.map(ve,(v,k)=>{return [k,v];}))}));
            throw new Error("Invalid Record: " + unwrapped);
        }
    }

    formatRecord(record){
        return {Data: record + '\n'};
    }

    putRecord(record){
        return this.putRecords([record]);
    }

    putRecords(records){
        _.map(records, this.validateRecord.bind(this));
        records = _.map(records, this.formatRecord);
        let chunks = _.chunk(records, this.maxIngestion);
        let tasks = [];
        for (let i=0; i < chunks.length; i++){
            tasks.push(this.drain.bind(this, chunks[i]));
        }
        return new Promise((resolve, reject) => {
            async.parallelLimit(tasks, this.maxDrains, function(err, results){
                err ? reject(err) : resolve(results);
            });
        });
    }

    drain(records, cb, numRetries=0){
        var leftovers = [];
        this.firehose.putRecordBatch({Records: records}, function(firehoseErr, resp){
            // Stuff broke!
            if (firehoseErr){
                // console.error(firehose_err);
                // console.dir(resp);
                return cb(firehoseErr);
            }

            // Not all records make it in, but firehose keeps on chugging!
            if (resp.FailedPutCount > 0){
                // console.log(resp.FailedPutCount + " rows failed, trying again...");
            }

            // Push errored records back into the next list.
            for (let [orig, result] of _.zip(records, resp.RequestResponses)){
                if (!_.isUndefined(result.ErrorCode)){
                    leftovers.push(orig);
                }
            }

            // Recurse!
            if (leftovers.length){
                return setTimeout(function(){
                    this.drain(leftovers, cb, numRetries + 1);
                }, this.retryInterval);
            } else {
                return cb(null);
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
    }

    putRecords(records){
        this.queue.push(...records);

        return new Promise((resolve, reject)=>{
            if (this.timeout === null){
                // Start the countdown timer since we've not already done so.
                this.timeout = setTimeout(()=>{
                    super.putRecords(this.queue)
                    .then((results)=>{resolve(results);})
                    .catch((err)=>{reject(err)});
                }, this.maxTime);
            } else {
                if (this.queue.length >= this.maxSize){
                    // Queue's full!
                    if (this.timeout !== null){
                        clearTimeout(this.timeout);
                        this.timeout = null;
                    }
                    super.putRecords(this.queue)
                    .then((results)=>{resolve(results);})
                    .catch((err)=>{reject(err)});
                }
            }
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
