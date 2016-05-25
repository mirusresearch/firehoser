var _ = require("lodash");
var expect = require('chai').expect;
var firehoser = require('./firehoser');

class MockAWSFirehose {
    putRecordBatch(batch, cb){
        cb(null, {
            FailedPutCount: 0,
            RequestResponses: batch.Records.slice()
        });
    }
}

describe("DeliveryStream", function(){

    describe("instantiation", function(){
        it("should only require 1 argument", function(){
            let ds = new firehoser.DeliveryStream("the_ds");
            expect(ds).to.be.an.instanceof(firehoser.DeliveryStream);
        });
    }),

    describe("DeliveryStream", function(){
        let ds = new firehoser.DeliveryStream("the_ds", null, null, 3000, new MockAWSFirehose());
        let jds = new firehoser.JSONDeliveryStream(
            "the_json_ds",
            null,
            {
                "type": "object",
                "properties": {
                    "firstName": {
                        "type": "string",
                        "maxLength": 3
                    },
                },
                "required": ["firstName"]
            },
            3000,
            new MockAWSFirehose()
        );

        it("should accept one record", function(){
            return ds.putRecord("this is the record");
        });

        it("should accept multiple records", function(){
            return ds.putRecords(["record1", "record2"]);
        });

        it("should accept more than 500 records", function(){
            return ds.putRecords(_.map(_.range(600), (i) => {`record${i}`}));
        });

        it("should throw an error when a record doesn't match the schema", function(){
            jds.putRecord({
                "firstName": 3
            }).catch((err, invalidRecords)=>{
                expect(err.type).to.be("schema");
                expect(err.details).to.exist;
                expect(err.originalRecord).to.exist;
            })
        });

        it("should not throw an error when the record matches the schema", function(){
            jds.putRecord({
                "firstName": "Don"
            }).catch((err, invalidRecords)=>{
                expect(err).to.not.exist;
            })
        });
    }),

    describe("QueuableDeliveryStream", function(){
        it("should call drain after a configurable timeout");
        it("should call drain after a maximum number of records have been put");
    })
})
