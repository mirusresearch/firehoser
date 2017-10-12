var _         = require("lodash");
var expect    = require('chai').expect;
var firehoser = require('./firehoser');

class MockAWSFirehose {
    constructor(firehoseErrors,failedPut){
        this.firehoseErrors = firehoseErrors || false;
        this.failedPut = failedPut || false;
        this.attempt = 0;
    }
    putRecordBatch(batch, cb){
        const err = this.firehoseErrors?new Error('OH NOES! Firehose Error!'):null;
        this.attempt++;
        // console.log('attempt!:',this.attempt);
        let reqresp = batch.Records.slice();
        let failedCount = 0;
        if (this.failedPut && this.attempt<2){
            reqresp     = _.map(reqresp,(r)=>{ return {ErrorMessage : 'Life is terrible', ErrorCode : 403 }; });
            failedCount = reqresp.length;
        }
        cb(err, {
            FailedPutCount   : failedCount,
            RequestResponses : reqresp
        });
    }
}

describe("DeliveryStream", function(){

    describe("instantiation", function(){
        it("should only require 1 argument", function(){
            let ds = new firehoser.DeliveryStream("the_ds");
            expect(ds).to.be.an.instanceof(firehoser.DeliveryStream);
        });
    });

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
            return jds.putRecord({
                "firstName": 3
            }).catch((errors)=>{
                let err = errors[0];
                expect(err.type).to.equal("schema");
                expect(err.details).to.exist;
                expect(err.originalRecord).to.exist;
            })
        });

        it("should not throw an error when the record matches the schema", function(){
            return jds.putRecord({
                "firstName": "Don"
            }).catch((errors)=>{
                expect(errors).to.be.undefined;
                expect(errors).to.be.empty;
            })
        });

        it("should handle firehose errors", function(){
            let fherr = new firehoser.DeliveryStream("the_ds", null, null, 1000, new MockAWSFirehose(true));
            return fherr.putRecord({ "firstName": "Don" }).catch((errors)=>{
                expect(errors).to.not.be.empty;
                expect(errors.length).to.equal(1);
            });
        });

        it("should retry failed PUTS", function(){
            let puterr = new firehoser.DeliveryStream("the_ds", null, null, 100, new MockAWSFirehose(false,true));
            return puterr.putRecord({ "firstName": "Don" });
        });


    });

})

describe('pickData', function(){
  let leftovers = [{
      type: "firehose",
      description: "out of service",
      details: {
          ErrorCode: 500,
          ErrorMessage: "AWS is currently down",
      },
      originalRecord: {
        Data: {log: true, data: { id: 1}}
      }
    },
    {
      type: "firehose",
      description: "out of service",
      details: {
          ErrorCode: 500,
          ErrorMessage: "AWS is currently down",
      },
      originalRecord: {
        Data: {log: true, data: { id: 2}}
      }
    }];

    it("should return the object with Data attributes", function(){
      let resolvedLeftoverData = _.map(leftovers, firehoser.pickData);

      expect(resolvedLeftoverData.length).to.equal(2);
      expect(resolvedLeftoverData[0]).to.have.property('Data');
      expect(resolvedLeftoverData[1]).to.have.property('Data');
    });
})
