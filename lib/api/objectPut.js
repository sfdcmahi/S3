const async = require('async');
const { errors, versioning } = require('arsenal');

const aclUtils = require('../utilities/aclUtils');
const { cleanUpBucket } = require('./apiUtils/bucket/bucketCreation');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const createAndStoreObject = require('./apiUtils/object/createAndStoreObject');
const { checkQueryVersionId } = require('./apiUtils/object/versioning');
const { metadataValidateBucketAndObj, getNullVersion } =
    require('../metadata/metadataUtils');
const { pushMetric } = require('../utapi/utilities');
const kms = require('../kms/wrapper');

const versionIdUtils = versioning.VersionID;

/**
 * PUT Object in the requested bucket. Steps include:
 * validating metadata for authorization, bucket and object existence etc.
 * store object data in datastore upon successful authorization
 * store object location returned by datastore and
 * object's (custom) headers in metadata
 * return the result in final callback
 *
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {request} request - request object given by router,
 *                            includes normalized headers
 * @param {object | undefined } streamingV4Params - if v4 auth,
 * object containing accessKey, signatureFromRequest, region, scopeDate,
 * timestamp, and credentialScope
 * (to be used for streaming v4 auth if applicable)
 * @param {object} log - the log request
 * @param {Function} callback - final callback to call with the result
 * @return {undefined}
 */
function objectPut(authInfo, request, streamingV4Params, log, callback) {
    log.debug('processing request', { method: 'objectPut' });
    if (!aclUtils.checkGrantHeaderValidity(request.headers)) {
        log.trace('invalid acl header');
        return callback(errors.InvalidArgument);
    }
    const queryContainsVersionId = checkQueryVersionId(request.query);
    if (queryContainsVersionId instanceof Error) {
        return callback(queryContainsVersionId);
    }
    const bucketName = request.bucketName;
    const objectKey = request.objectKey;
    const requestType = 'objectPut';
    const valParams = { authInfo, bucketName, objectKey, requestType };
    const canonicalID = authInfo.getCanonicalID();
    log.trace('owner canonicalID to send to data', { canonicalID });

    return metadataValidateBucketAndObj(valParams, log,
    (err, bucket, objMD) => {
        const responseHeaders = collectCorsHeaders(request.headers.origin,
            request.method, bucket);
        if (err) {
            log.trace('error processing request', {
                error: err,
                method: 'metadataValidateBucketAndObj',
            });
            return callback(err, responseHeaders);
        }
        if (bucket.hasDeletedFlag() && canonicalID !== bucket.getOwner()) {
            log.trace('deleted flag on bucket and request ' +
                'from non-owner account');
            return callback(errors.NoSuchBucket);
        }
        return async.waterfall([
            function getOldByteLength(next) {
                const vCfg = bucket.getVersioningConfiguration();
                const versioningStatus = vCfg ? vCfg.Status : undefined;
                // Utapi expects null or a number for oldByteLength:
                // * null - new object or version
                // * 0 or > 0 - existing object with content-length 0 or > 0
                // objMD here is the master (latest) version that we would
                // have overwritten if there was an existing object
                if (!objMD || versioningStatus === 'Enabled') {
                    return process.nextTick(next, null, versioningStatus, null);
                }
                if (versioningStatus === undefined) {
                    const oldByteLength =
                        objMD['content-length'] !== undefined ?
                        objMD['content-length'] : null;
                    return process.nextTick(next, null, versioningStatus,
                        oldByteLength);
                }
                // If versioning is suspended, putting a new object overwrites
                // the previous null version, which may or or may not be the
                // master version. To send the correct previous content length
                // to Utapi, we need to be sure to retrieve the correct version.
                return getNullVersion(objMD, bucketName, objectKey, log,
                    (err, nullVerMD) => {
                        if (err) {
                            log.error('unexpected error retrieving null ' +
                            'version metadata', { error: err });
                            return next(errors.InternalError);
                        }
                        const oldByteLength = nullVerMD &&
                            nullVerMD['content-length'] !== undefined ?
                            nullVerMD['content-length'] : null;
                        return next(null, versioningStatus, oldByteLength);
                    });
            },
            function handleTransientOrDeleteBuckets(vStatus, oldByteLength,
                next) {
                if (bucket.hasTransientFlag() || bucket.hasDeletedFlag()) {
                    return cleanUpBucket(bucket, canonicalID, log, err =>
                        next(err, vStatus, oldByteLength));
                }
                return next(null, vStatus, oldByteLength);
            },
            function createCipherBundle(vStatus, oldByteLength, next) {
                const serverSideEncryption = bucket.getServerSideEncryption();
                if (serverSideEncryption) {
                    return kms.createCipherBundle(
                        serverSideEncryption, log, (err, cipherBundle) =>
                        next(err, cipherBundle, vStatus, oldByteLength));
                }
                return next(null, null, vStatus, oldByteLength);
            },
            function objectCreateAndStore(cipherBundle, vStatus, oldByteLength,
                next) {
                return createAndStoreObject(bucketName,
                bucket, objectKey, objMD, authInfo, canonicalID, cipherBundle,
                request, false, streamingV4Params, log, (err, storingResult) =>
                next(err, storingResult, vStatus, oldByteLength));
            },
        ], (err, storingResult, versioningStatus, oldByteLength) => {
            if (err) {
                return callback(err, responseHeaders);
            }
            const isVersionedObj = versioningStatus === 'Enabled';
            if (isVersionedObj && storingResult && storingResult.versionId) {
                responseHeaders['x-amz-version-id'] =
                    versionIdUtils.encode(storingResult.versionId);
            }
            if (storingResult) {
                // ETag's hex should always be enclosed in quotes
                responseHeaders.ETag = `"${storingResult.contentMD5}"`;
            }
            pushMetric('putObject', log, {
                authInfo,
                bucket: bucketName,
                newByteLength: request.parsedContentLength,
                oldByteLength,
            });
            return callback(null, responseHeaders);
        });
    });
}

module.exports = objectPut;
