const AWS = require('aws-sdk');
const async = require('async');
const UUID = require('uuid/v4');
const { errors } = require('arsenal');
const { removeQuotes } = require('../utils');
const request = require('request');
const Service = AWS.Service;

const assert = require('assert');

const GcpSigner = require('./GcpSigner');

AWS.apiLoader.services.gcp = {};
const GCP = Service.defineService('gcp', ['2017-11-01']);
Object.defineProperty(AWS.apiLoader.services.gcp, '2017-11-01', {
    get: function get() {
        const model = require('./gcp-2017-11-01.api.json');
        return model;
    },
    enumerable: true,
    configurable: true,
});

Array.prototype.eachSlice = function (size) {
    this.array = [];
    let partNumber = 1;
    for (let ind = 0; ind < this.length; ind += size) {
        this.array.push( {
            Parts: this.slice(ind, ind + size),
            PartNumber: partNumber++,
        });
    }
    return this.array;
};

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
        // if partNumber is given, return a "full file path"
        // else return a "directory path"
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

function _createMpuList(params, level, size) {
    // populate and return a parts list for compose
    const retList = [];
    for (let i = 1; i <= size; ++i) {
        retList.push({
            PartName: `${params.Key}-${params.UploadId}/${level}/${i}`,
            PartNumber: i,
        });
    }
    return retList;
}

function splitMerge(params, partList, level, callback) {
    // create composition of slices from the partList array
    console.log(partList.length);
    let count = 0;
    return async.mapLimit(partList.eachSlice(32), 3, (infoParts, cb) => {
        const mpuPartList = infoParts.Parts.map(item => {
            return { PartName: item.PartName } });
        const partNumber = infoParts.PartNumber;
        const tmpKey =
            _createMpuKey(params.Key, params.UploadId, partNumber, level);
        const mergedObject = { PartName: tmpKey };
        if (mpuPartList.length < 2) {
            // else just perform a copy
            const copyParams = {
                Bucket: params.MPU,
                Key: tmpKey,
            };
            return this.copyObject(copyParams, (err, res) => {
                if (err) {
                    return cb(err);
                }
                mergedObject.VersionId = res.VersionId;
                mergedObject.ETag = res.ETag;
                return cb(null, mergedObject);
            });
        }
        console.log(`\r${++count}`);
        const composeParams = {
            Bucket: params.MPU,
            Key: tmpKey,
            MultipartUpload: { Parts: mpuPartList },
        };
        return this.composeObject(composeParams, (err, res) => {
            if (err) {
                return cb(err);
            }
            mergedObject.VersionId = res.VersionId;
            mergedObject.ETag = res.ETag;
            return cb(null, mergedObject);
        });
    }, (err, res) => {
        if (err) {
            return callback(err);
        }
        return callback(null, res.length);
    });
}

Object.assign(GCP.prototype, {

    getSignerClass() {
        return GcpSigner;
    },

    validateService() {
        if (!this.config.region) {
            this.config.region = 'us-east-1';
        }
    },

    upload(params, options, callback) {
        /* eslint-disable no-param-reassign */
        if (typeof options === 'function' && callback === undefined) {
            callback = options;
            options = null;
        }
        options = options || {};
        options = AWS.util.merge(options, { service: this, params });
        /* eslint-enable no-param-reassign */

        const uploader = new AWS.S3.ManagedUpload(options);
        if (typeof callback === 'function') uploader.send(callback);
        return uploader;
    },

    putObjectTagging(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: putObjectTagging not implementend'));
    },

    deleteObjectTagging(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: deleteObjectTagging not implementend'));
    },

    _removeParts(params, callback) {
        // marks live objects as archived for lifecycle to handle the deletions
        // delete objects from mpu bucket and overflow bucket
        return async.parallel([
            done => {
                // delete mpu bucket
                const isDone = false;
                return async.doWhilst(() => {
                    const delParams = {
                        Bucket: params.MPU,
                        Prefix: params.Prefix,
                    };
                    return this.listObjects()
                })
            },
            done => {
                // delete overflow
                const delParams = {
                    Bucket: params.Overflow,
                    Prefix: params.Prefix,
                };

            },
        ], (err, res) => {
            if (err) {
            }
        })
    },

    abortMultipartUpload(params, callback) {
        const delParams = {
            Bucket: params.Bucket,
            MPU: params.MPU,
            Overflow: params.Overflow,
            Prefix: _createMpuKey(params.Key, params.UploadId),
        };
        return this._removeParts(delParams, err => {
            if (err) {
                return callback(err);
            }
            return callback();
        });
    },

    createMultipartUpload(params, callback) {
        // As google cloud does not have a create MPU function,
        // create an empty 'init' object that will temporarily store the
        // object metadata and return an upload ID to mimic an AWS MPU
        const uploadId = UUID().replace(/-/g, '');
        const mpuParams = {
            Bucket: params.Bucket,
            Key: _createMpuKey(params.Key, uploadId, 'init'),
            Metadata: params.MetaHeaders,
            ContentType: params.ContentType,
            CacheControl: params.CacheControl,
            ContentDisposition: params.ContentDisposition,
            ContentEncoding: params.ContentEncoding,
        };
        return this.putObject(mpuParams, (err, res) => {
            if (err) {
                return callback(err);
            }
            return callback(null, { UploadId: uploadId });
        });
    },

    listParts(params, callback) {
        const mpuParams = {
            Bucket: params.Bucket,
            Prefix: _createMpuKey(params.Key, params.UploadId, 'parts'),
            MaxKeys: params.MaxParts,
        };
        return this.listObjects(mpuParams, (err, res) => {
            if (err) {
                return callback(err);
            }
            return callback(null, res);
        });
    },

    completeMultipartUpload(params, callback) {
        const partList = params.MultipartUpload.Parts;
        // verify that the part list is in order
        for (let ind = 1; ind < partList.length; ++ind) {
            if (partList[ind - 1].PartNumber >= partList[ind].PartNumber) {
                return callback(errors.InvalidPartOrder);
            }
        }
        return async.waterfall([
            next => {
                process.stdout.write('first compose level')
                // first compose: in mpu bucket
                // max 10,000 => 313 parts
                // max component count per object 32
                return splitMerge.call(this, params, partList, 'mpu1', next);
            },
            (numParts, next) => {
                process.stdout.write('\rsecond compose level')
                // second compose: in mpu bucket
                // max 313 => 10 parts
                // max component count per object 1024
                const parts = _createMpuList(params, 'mpu1', numParts);
                return splitMerge.call(this, params, parts, 'mpu2', next);
            },
            (numParts, next) => {
                process.stdout.write('\roverflow copy level')
                // copy phase: in overflow bucket
                // resetting component count by moving item between
                // different region/class buckets
                const parts = _createMpuList(params, 'mpu2', numParts);
                return async.map(parts, (infoParts, cb) => {
                    const partName = infoParts.PartName;
                    const partNumber = infoParts.PartNumber;
                    const overflowKey = _createMpuKey(
                        params.Key, params.UploadId, partNumber, 'overflow');
                    const copyParams = {
                        Bucket: params.Overflow,
                        Key: overflowKey,
                        CopySource: `${params.MPU}/${partName}`,
                    };
                    const copyObject = { PartName: overflowKey };
                    this.copyObject(copyParams, (err, res) => {
                        if (err) {
                            return cb(err);
                        }
                        copyObject.VersionId = res.VersionId;
                        copyObject.ETag = res.ETag;
                        return cb(null, copyObject);
                    });
                }, (err, res) => {
                    if (err) {
                        return next(err);
                    }
                    return next(null, res.length);
                });
            },
            (numParts, next) => {
                process.stdout.write('\roverflow compose level')
                // final compose: in overflow bucket
                // number of parts to compose <= 10
                // perform final compose in overflow bucket
                const parts = _createMpuList(params, 'overflow', numParts);
                const partList = parts.map(item => {
                    return { PartName: item.PartName } });
                console.log(partList.length);
                if (partList.length < 2) {
                    return next(partList[0].PartName);
                }
                const composeParams = {
                    Bucket: params.Overflow,
                    Key: params.Key,
                    MultipartUpload: { Parts: partList },
                };
                return this.composeObject(composeParams, (err, res) => {
                    if (err) {
                        return next(err);
                    }
                    return next();
                });
            },
            (res, next) => {
                process.stdout.write('\rfinal copy level\n')
                // move object from overflow bucket into the main bucket
                // retrieve initial metadata then compose the object
                return async.waterfall([
                    next => {
                        // retrieve metadata from init object in mpu bucket
                        const headParams = {
                            Bucket: params.MPU,
                            Key: _createMpuKey(params.Key, params.UploadId,
                                'init'),
                        };
                        return this.headObject(headParams, (err, res) => {
                            if (err) {
                                return next(err);
                            }
                            return next(null, res.Metadata);
                        });
                    },
                    (metadata, next) => {
                        // copy the final object into the main bucket
                        const copySource = res ? `${params.Overflow}/${res}` :
                            `${params.Overflow}/${params.Key}`;
                        const copyParams = {
                            Bucket: params.Bucket,
                            Key: params.Key,
                            Metadata: metadata,
                            MetadataDirective: 'REPLACE',
                            CopySource: copySource,
                        };
                        this.copyObject(copyParams, (err, res) => {
                            if (err) {
                                return next(err);
                            }
                            return next(null, res);
                        });
                    },
                ], (err, mpuResult) => {
                    // removing objects
                    process.stdout.write('removing tmp objects');
                    if (err) {
                        return next(err);
                    }
                    // const delParams = {
                    //     Bucket: params.Bucket,
                    //     MPU: params.MPU,
                    //     Overflow: params.Overflow,
                    //     Prefix: _createMpuKey(params.Key, params.UploadId),
                    // };
                    // return this._removeParts(delParams, (err, res) => {
                    //     if (err) {
                    //         return next(err);
                    //     }
                        return next(null, mpuResult);
                    // });
                });
            },
        ], (err, mpuResult) => {
            if (err) {
                return callback(err);
            }
            return callback(null, mpuResult);
        });
    },

    uploadPart(params, callback) {
        const mpuParams = {
            Bucket: params.Bucket,
            Key: _createMpuKey(params.Key, params.UploadId, params.PartNumber),
            Body: params.Body,
            ContentLength: params.ContentLength,
        };
        return this.putObject(mpuParams, (err, res) => {
            if (err) {
                return callback(err);
            }
            return callback(null, res);
        });
    },

    uploadPartCopy(params, callback) {
        const mpuParams = {
            Bucket: params.Bucket,
            Key: _createMpuKey(params.Key, params.UploadId, params.PartNumber),
            CopySource: params.CopySource, 
        };
        return this.copyObject(mpuParams, (err, res) => {
            if (err) {
                return callback(err);
            }
            const CopyPartObject = { CopyPartResult: res };
            return callback(null, CopyPartObject);
        });
    },
});

module.exports = GCP;
