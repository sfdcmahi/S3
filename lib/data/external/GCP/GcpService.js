const AWS = require('aws-sdk');
const async = require('async');
const { errors } = require('arsenal');
const { removeQuotes } = require('../utils');
const request = require('request');
const Service = AWS.Service;

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

function getRandomInt(min, max) {
  min = Math.ceil(min);
  max = Math.floor(max);
  return Math.floor(Math.random() * (max - min)) + min;
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
        /* eslint-disable no-param-reassign */

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
        return this.listObjectVersions(params, (err, versionResults) => {
            if (err) {
                return callback(err);
            }
            const versionList = versionResults.Versions;
            return async.map(versionList, (item, end) => {
                const delParam = {
                    Bucket: params.Bucket,
                    Key: item.Key,
                    VersionId: item.VersionId,
                };
                setTimeout(() => {
                    this.deleteObject(delParam, err => {
                        if (err) {
                            return end(err);
                        }
                        return end();
                    });
                }, getRandomInt(100, 1000));
            }, err => {
                if (err) {
                    return callback(err);
                }
                return callback();
            });
        });
    },

    abortMultipartUpload(params, callback) {
        const delParams = {
            Bucket: params.gcpBucket,
            Key: params.gcpKey,
        };
        const partsParam = {
            Bucket: params.gcpBucket,
            Prefix: params.Prefix,
        };
        return async.parallel([
            end => {
                this.deleteObject(delParams, err => {
                    if (err) {
                        end(err);
                    }
                    return end();
                });
            },
            end => {
                this._removeParts(partsParam, err => {
                    if (err) {
                        end(err);
                    }
                    return end();
                });
            },
        ], err => {
            if (err) {
                return callback(err);
            }
            return callback();
        });
    },

    createMultipartUpload(params, callback) {
        return this.putObject(params, (err, res) => {
            if (err) {
                return callback(err);
            }
            return callback(null, res);
        })
    },

    listParts(params, callback) {
        return this.listObjects(params, (err, res) => {
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
            if (partList[ind - 1].PartName >= partList[ind].PartName)
                return callback(errors.InvalidPartOrder);
        }
        return async.map(partList.eachSlice(32), (infoParts, cb) => {
            const partList = infoParts.Parts.map(item => {
                return { PartName: item.PartName } });
            const partNumber = infoParts.PartNumber;
            const tmpKey = `${params.TmpDirectory}/final${partNumber}`;
            const composeParams = {
                Bucket: params.Bucket,
                Key: tmpKey,
                MultipartUpload: { Parts: partList },
            };
            const mergedObject = { PartName: tmpKey };
            this.composeObject(composeParams, (err, res) => {
                if (err) {
                    return cb(err);
                }
                mergedObject.PartNumber = partNumber;
                mergedObject.VersionId = res.VersionId;
                return cb(null, mergedObject);
            })
        }, (err, res) => {
            if (err) {
                return callback(err);
            }
            // res is an array of finalMerged object
            // sort the list for the final merge to the correct directory
            res.sort((a, b) => {
                if (a.PartNumber < b.PartNumber) {
                    return -1;
                }
                if (a.PartNumber > b.PartNumber) {
                    return 1;
                }
                return 0;
            });
            const partList = res.map(item => {
                return { PartName: item.PartName } });
            const composeParams = {
                Bucket: params.Bucket,
                Key: params.Key,
                MultipartUpload: { Parts: partList },
            };
            this.composeObject(composeParams, (err, finalComposeRes) => {
                if (err) {
                    return callback(err);
                }
                const cleanParams = {
                    Bucket: params.Bucket,
                    Prefix: `${params.TmpDirectory}/part`,
                };
                this._removeParts(cleanParams, err => {
                    if (err) {
                        return callback(err);
                    }
                    return callback(null, finalComposeRes);
                });
            });
        });
    },

    uploadPartCopy(params, callback) {
        return this.copyObject(params, (err, res) => {
            if (err) {
                return callback(err);
            }
            const CopyPartObject = { CopyPartResult: res };
            return callback(null, CopyPartObject);
        });
    },
});

module.exports = GCP;
