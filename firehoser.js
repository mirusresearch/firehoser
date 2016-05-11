'use strict';
var _      = require('../node_modules/lodash');
var AWS    = require('../node_modules/aws-sdk');
var async  = require('../node_modules/async');


class DeliveryStream{
    constructor(name, retryInterval=1500, awsConfig=null){
        this.name = name;
        this.retryInterval = retryInterval;
        if (awsConfig !== null){
            AWS.config.update(awsConfig);
        }
        this.firehose = new AWS.Firehose({params: {DeliveryStreamName: name}});
        this.max_ingestion = 400;
        this.max_drains = 3;
    }

    formatRecord(record){
        return {Data: record + '\n'};
    }

    putRecord(record){
        return this.putRecords([record]);
    }

    putRecords(records){
        records = _.map(records, this.formatRecord);
        let chunks = _.chunk(records, this.max_ingestion);
        let tasks = [];
        for (let i=0; i < chunks.length; i++){
            tasks.push(this.drain.bind(this, chunks[i]));
        }
        return new Promise((resolve, reject) => {
            async.parallelLimit(tasks, this.max_drains, function(err, results){
                err ? reject(err) : resolve(results);
            });
        });
    }

    drain(records, cb, numRetries=0){
        var leftOvers = [];
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

            // Push errored out records back into the next list.
            for (let [orig, result] of _.zip(records, resp.RequestResponses)){
                if (!_.isUndefined(result.ErrorCode)){
                    leftOvers.push(orig);
                }
            }

            // Recurse!
            if (leftOvers.length){
                return setTimeout(function(){
                    this.drain(leftOvers, cb, numRetries + 1);
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
    constructor(name, retryInterval, aws_config, maxTime=30000, maxSize=500, ...args){
        super(name, retryInterval, aws_config, ...args);
        this.queue = [];
        this.timeout = null;
        this.maxTime = maxTime;
        this.maxSize = maxSize;
    }

    putRecords(records){
        this.queue.push(...records);

        return new Promise((resolve, reject)=>{
            if (this.timeout === null){
                this.timeout = setTimeout(()=>{
                    super.putRecords(this.queue)
                    .then((results)=>{resolve(results);})
                    .catch((err)=>{reject(err)});
                }, this.maxTime);
                return;
            }
            if (this.queue.length >= this.maxSize){
                if (this.timeout !== null){
                    clearTimeout(this.timeout);
                    this.timeout = null;
                }
                super.putRecords(this.queue)
                .then((results)=>{resolve(results);})
                .catch((err)=>{reject(err)});
                return;
            }
        });
    }
}

class QueuableJSONDeliveryStream extends QueuableDeliveryStream {
    formatRecord(record){
        return super.formatRecord(JSON.stringify(record));
    }
}
