var _ = require("lodash");
var expect = require('chai').expect;
var firehoser = require('./firehoser');

describe("DeliveryStream", function(){

	describe("instantiation", function(){
		it("should only require 1 argument", function(){
			let ds = new firehoser.DeliveryStream("the_ds");
			expect(ds).to.be.an.instanceof(firehoser.DeliveryStream);
		});
	}),

	describe("DeliveryStream", function(){
		let ds = new firehoser.DeliveryStream("the_ds");
		let jds = new firehoser.JSONDeliveryStream("the_json_ds", null, {
			"type": "object",
			"properties": {
				"firstName": {
					"type": "string",
					"maxLength": 3
				},
			},
			"required": ["firstName"]
		});

		it("should accept one record", function(){
			return ds.putRecord("this is the record");
		});

		it("should accept multiple records", function(){
			return ds.putRecords(["record1", "record2"]);
		});

		it("should accept more than 500 records", function(){
			return ds.putRecords(_.map(_.range(600), (i) => {`record${i}`}));
		});

		it("should validate records match a schema", function(){
			ds.putRecord({
				"lastName": "DoesntMatter"
			}).catch((err)=>{
				expect(err).to.exist;
			})

			ds.putRecord({
				"firstName": "Don"
			}).catch((err)=>{
				expect(err).to.not.exist;
			})
		});
	}),

	describe("QueuableDeliveryStream", function(){
		it("should call drain after a configurable timeout");
		it("should call drain after a maximum number of records have been put");
	})
})