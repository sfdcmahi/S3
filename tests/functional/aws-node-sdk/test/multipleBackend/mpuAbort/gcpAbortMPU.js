const assert = require('assert');
const async = require('async');

const { s3middleware } = require('arsenal');
const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');
const { describeSkipIfNotMultiple, gcpS3, gcpLocation, convertMD5 }
    = require('../utils');

const keyObject = 'gcpAbort';
const bucket = 'gcpmultiplebackendmpuabort';
const expectedMD5 = 'a63c90cc3684ad8b0a2176a6a8fe9005';

let bucketUtil;
let s3;

function gcpCheck(bucket, key, expected, cb) {
    const params = {
        Bucket: bucket,
        Key: key,
    };
    gcpS3.headObject(params, (err, res) => {
        if (expected.error) {
            assert.strictEqual(err.statusCode, 404);
            assert.strictEqual(err.code, 'NotFound');
        } else {
            assert.strictEqual(res.contentMD5, expectedMD5);
        }
        return cb();
    });
}

describeSkipIfNotMultiple('Abort MPU on GCP data backend', function
describeF() {
    this.timeout(50000);
    withV4(sigCfg => {
        beforeEach(function beforeFn() {
            this.currentTest.key = `somekey-${Date.now()}`;
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
        });
    })
});