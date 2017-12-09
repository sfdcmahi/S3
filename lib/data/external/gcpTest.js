const assert = require('assert');
const UUID = require('uuid/v4');
const randomstring = require('randomstring');
const rp = require('bluebird');
const async = require('async');
const GCP = require('./GCP');

// full permissions
const g1Params= {
    endpoint: 'https://storage.googleapis.com',
    accessKeyId: 'GOOGJBKQ75A4TEXQCT7S',
    secretAccessKey: '1abAKfN9uZ2mOzDApqRU2xSpom3ak4xRRb4hJflx',
};
const client1 = rp.promisifyAll(new GCP(g1Params));

// secondary bucket only permission
const g2Params= {
    endpoint: 'https://storage.googleapis.com',
    accessKeyId: 'GOOG3UHJPRVSH2GVFT7K',
    secretAccessKey: 'EyhhB0cm4Be6pL4WlgH/LrjwT6+22G5avtaxnz0Z',
};
const client2 = rp.promisifyAll(new GCP(g2Params));

const bucketC1 = 'zenko-bucket-555';
const bucketC2 = 'zenko-bucket-222';
const bucketMPU = 'zenko-bucket-mpu';
const bucketOverflow = 'zenko-bucket-overflow';
const itemLength = 5;

function _getRandomInt(min, max) {
  min = Math.ceil(min);
  max = Math.floor(max);
  return Math.floor(Math.random() * (max - min)) + min;
}

function _createMpuKey(key, uploadId, partNumber, fileName) {
    /* eslint-disable no-param-reassign */ 
    if (typeof partNumber === 'string' && fileName === undefined) {
        fileName = partNumber;
        partNumber = null;
    }
    /* esline-enable no-param-reassign */
    if (fileName && typeof fileName === 'string') {
        return partNumber ? `${key}-${uploadId}/${fileName}/${partNumber}` :
            `${key}-${uploadId}/${fileName}`;
    };
    if (partNumber && typeof partNumber === 'number') {
        // filename wasn't passed as an argument. Create default
        return `${key}-${uploadId}/parts/${partNumber}`;
    }
    // returns a "directory parth"
    return `${key}-${uploadId}/`;
}

describe('Tests GCP service client', function testFn() {
    it('Tests that client2 should not be able to access anyting aside ' +
    'zenko-bucket-555', callback => {
        return client2.listObjects({
            Bucket: 'zenko-bucket-555',
        }, (err, res) => {
            assert.strictEqual(err.code, 'AccessDenied', 'Should have failed');
            return callback();
        });
    });

    it('list parts', done => {
        return client1.listObjects({
            Bucket: 'zenko-bucket-555',
        }, (err, res) => {
            console.log(res.Parts);
            return done();
        });
    });

    describe('MPU Test', function testFn() {
        this.timeout(600000);
        beforeEach('Initiate a MPU', function testFn(done) {
            this.currentTest.key = randomstring.generate({ length: 10 });
            const mpuParams = {
                Bucket: bucketMPU,
                Key: this.currentTest.key,
                MetaHeaders: {
                    'test': '123',
                },
            };
            return client1.createMultipartUpload(mpuParams,(err, result) => {
                if (err)
                    process.stdout.write(`Error: ${err}\n`);
                if (result) {
                    this.currentTest.uploadId = result.UploadId;
                    const headParams = {
                        Bucket: bucketMPU,
                        Key: _createMpuKey(this.currentTest.key, this.currentTest.uploadId,'init'),
                    };
                    return client1.headObject(headParams, (err, res) => {
                        if (err) {
                            console.log(err);
                        }
                        console.log(res);
                        return done();
                    });
                }
            });
        });

        it.only('MPU: 10000 Parts', function testFn(done) {
            const range = [];
            for (let i = 1; i <= 2000; i++) {
                range.push(i);
            }
            return async.waterfall([
                next => {
                    return async.mapLimit(range, 200, (id, next) => {
                        const putParams = {
                            Bucket: bucketMPU,
                            Key: this.test.key,
                            UploadId: this.test.uploadId,
                            PartNumber: id,
                            Body: Buffer.from(randomstring.generate({
                                length: itemLength })
                            ),
                            ContentLength: itemLength,
                        };
                        return client1.uploadPart(putParams, (err, res) => {
                            if (err) {
                                console.log(err);
                                return next('Unable to put part');
                            }
                            process.stdout.write(`\rUploaded Part${id}`);
                            return next(null, {
                                PartName: _createMpuKey(this.test.key, this.test.uploadId, id),
                                PartNumber: id,
                                ETag: res.ETag,
                            });
                        });
                    }, (err, parts) => {
                        if (err) {
                            return next(err);
                        }
                        process.stdout.write('\rUpload Complete\n');
                        return next(null, parts);
                    });
                },
                (parts, next) => {
                    const comParams = {
                        Bucket: bucketC1,
                        MPU: bucketMPU,
                        Overflow: bucketOverflow,
                        Key: this.test.key,
                        UploadId: this.test.uploadId,
                        TmpDirectory: `${this.test.key}-${this.test.uploadId}`,
                        MultipartUpload: { Parts: parts },
                    };
                    return client1.completeMultipartUpload(comParams,
                    (err, res) => {
                        if (err) {
                            return next(err);
                        }
                        return next(null, res);
                    });
                }
            ], (err, res) => {
                if (err) {
                    console.log('error')
                    return done(err);
                }
                client1.headObject({ Bucket: bucketC1, Key: this.test.key},
                (err, res) => {
                    if (err) {
                        console.log('error')
                        return done(err);
                    }
                    console.log(res.ContentLength);
                });
                return done();
            });
        });
    });
});

// rmsjJkepy1-48905b8de78b48bda38dcea7b8a2015a
// client1.copyObject({ Bucket: bucketOverflow, Key: 'test', CopySource: 'zenko-bucket-mpu/BYUkZcj3n4-11ffc37efb8b4fa199ab2fcd535fb173/mpu2/2' },
// (err, res) => {
//     if (err)
//         console.log(err)
//     if (res) {
//         client1.copyObject({ Bucket: bucketMPU, Key: 'testing', CopySource: `${bucketOverflow}/test` },
//         (err, res) => {
//             client1.headObject({ Bucket: bucketMPU, Key: 'testing' },
//             (err, res) => {
//                 if (err){
//                     console.log('error');
//                     console.log(err)
//                 }
//                 if (res)
//                     console.log(res)
//             })
//         });
//         console.log(res)
//     }
// })

