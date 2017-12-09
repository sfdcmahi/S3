const assert = require('assert');
const async = require('async');

const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');
const { describeSkipIfNotMultiple, gcpS3, gcpLocation, gcpLocationMismatch,
    uniqName } = require('../utils');

const keyObject = 'putgcp';
const gpcBucket = 

let bucketUtil;
let s3;

describeSkipIfNotMultiple('MultipleBackend put part to GCP', () => {
    this.timeout(80000);
    withV4(sigCfg => {
        beforeEach(() => {
            this.currentTest.Key = uniqName(keyObject);
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
        });
        describe('with bucket location header', () => {
            beforeEach(function beforeEachFn(done) {
                async.waterfall([
                    next => s3.createBucket({ Bucket: gcpContainerName,
                    }, err => next(err)),
                    next => s3.createMultipartUpload({
                        
                    })
                ], (err, res) => {
                    if (err) {
                        return next(err);
                    }
                    this.currentTest.uploadId = res.UploadId;
                    return next();
                });
            });
        })
    });
})