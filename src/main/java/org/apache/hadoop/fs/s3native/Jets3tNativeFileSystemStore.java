// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3native;

import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.s3.S3Exception;
import org.apache.hadoop.security.AccessControlException;
import java.io.EOFException;
import org.jets3t.service.impl.rest.HttpException;
import org.jets3t.service.model.MultipartUpload;
import java.util.Collections;
import org.jets3t.service.model.MultipartPart;
import org.jets3t.service.StorageObjectsChunk;
import java.util.Calendar;
import java.io.FileNotFoundException;
import java.io.ByteArrayInputStream;
import org.jets3t.service.multi.s3.S3ServiceEventListener;
import java.util.List;
import org.jets3t.service.utils.MultipartUtils;
import org.jets3t.service.model.StorageObject;
import java.util.ArrayList;
import java.io.Closeable;
import org.apache.hadoop.io.IOUtils;
import org.jets3t.service.ServiceException;
import org.jets3t.service.model.S3Object;
import java.io.InputStream;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.File;
import java.io.IOException;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.security.ProviderCredentials;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.security.AWSCredentials;
import org.apache.hadoop.fs.s3.S3Credentials;
import org.apache.hadoop.conf.Configuration;
import java.net.URI;
import org.slf4j.Logger;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.S3Service;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceAudience;

@InterfaceAudience.Private
@InterfaceStability.Unstable
class Jets3tNativeFileSystemStore implements NativeFileSystemStore
{
    private S3Service s3Service;
    private S3Bucket bucket;
    private long multipartBlockSize;
    private boolean multipartEnabled;
    private long multipartCopyBlockSize;
    static final long MAX_PART_SIZE = 5368709120L;
    private String serverSideEncryptionAlgorithm;
    public static final Logger LOG;
    
    @Override
    public void initialize(final URI uri, final Configuration conf) throws IOException {
        final S3Credentials s3Credentials = new S3Credentials();
        s3Credentials.initialize(uri, conf);
        try {
            final AWSCredentials awsCredentials = new AWSCredentials(s3Credentials.getAccessKey(), s3Credentials.getSecretAccessKey());
            this.s3Service = (S3Service)new RestS3Service((ProviderCredentials)awsCredentials);
        }
        catch (S3ServiceException e) {
            this.handleException((Exception)e);
        }
        this.multipartEnabled = conf.getBoolean("fs.s3n.multipart.uploads.enabled", false);
        this.multipartBlockSize = Math.min(conf.getLong("fs.s3n.multipart.uploads.block.size", 67108864L), 5368709120L);
        this.multipartCopyBlockSize = Math.min(conf.getLong("fs.s3n.multipart.copy.block.size", 5368709120L), 5368709120L);
        this.serverSideEncryptionAlgorithm = conf.get("fs.s3n.server-side-encryption-algorithm");
        this.bucket = new S3Bucket(uri.getHost());
    }
    
    @Override
    public void storeFile(final String key, final File file, final byte[] md5Hash) throws IOException {
        if (this.multipartEnabled && file.length() >= this.multipartBlockSize) {
            this.storeLargeFile(key, file, md5Hash);
            return;
        }
        BufferedInputStream in = null;
        try {
            in = new BufferedInputStream(new FileInputStream(file));
            final S3Object object = new S3Object(key);
            object.setDataInputStream((InputStream)in);
            object.setContentType("binary/octet-stream");
            object.setContentLength(file.length());
            object.setServerSideEncryptionAlgorithm(this.serverSideEncryptionAlgorithm);
            if (md5Hash != null) {
                object.setMd5Hash(md5Hash);
            }
            this.s3Service.putObject(this.bucket, object);
        }
        catch (ServiceException e) {
            this.handleException((Exception)e, key);
        }
        finally {
            IOUtils.closeStream((Closeable)in);
        }
    }
    
    public void storeLargeFile(final String key, final File file, final byte[] md5Hash) throws IOException {
        final S3Object object = new S3Object(key);
        object.setDataInputFile(file);
        object.setContentType("binary/octet-stream");
        object.setContentLength(file.length());
        object.setServerSideEncryptionAlgorithm(this.serverSideEncryptionAlgorithm);
        if (md5Hash != null) {
            object.setMd5Hash(md5Hash);
        }
        final List<StorageObject> objectsToUploadAsMultipart = new ArrayList<StorageObject>();
        objectsToUploadAsMultipart.add((StorageObject)object);
        final MultipartUtils mpUtils = new MultipartUtils(this.multipartBlockSize);
        try {
            mpUtils.uploadObjects(this.bucket.getName(), this.s3Service, (List)objectsToUploadAsMultipart, (S3ServiceEventListener)null);
        }
        catch (Exception e) {
            this.handleException(e, key);
        }
    }
    
    @Override
    public void storeEmptyFile(final String key) throws IOException {
        try {
            final S3Object object = new S3Object(key);
            object.setDataInputStream((InputStream)new ByteArrayInputStream(new byte[0]));
            object.setContentType("binary/octet-stream");
            object.setContentLength(0L);
            object.setServerSideEncryptionAlgorithm(this.serverSideEncryptionAlgorithm);
            this.s3Service.putObject(this.bucket, object);
        }
        catch (ServiceException e) {
            this.handleException((Exception)e, key);
        }
    }
    
    @Override
    public FileMetadata retrieveMetadata(final String key) throws IOException {
        StorageObject object = null;
        try {
            Jets3tNativeFileSystemStore.LOG.debug("Getting metadata for key: {} from bucket: {}", (Object)key, (Object)this.bucket.getName());
            object = this.s3Service.getObjectDetails(this.bucket.getName(), key);
            return new FileMetadata(key, object.getContentLength(), object.getLastModifiedDate().getTime());
        }
        catch (ServiceException e) {
            try {
                this.handleException((Exception)e, key);
                return null;
            }
            catch (FileNotFoundException fnfe) {
                final FileMetadata fileMetadata = null;
                if (object != null) {
                    object.closeDataInputStream();
                }
                return fileMetadata;
            }
        }
        finally {
            if (object != null) {
                object.closeDataInputStream();
            }
        }
    }
    
    @Override
    public InputStream retrieve(final String key) throws IOException {
        try {
            Jets3tNativeFileSystemStore.LOG.debug("Getting key: {} from bucket: {}", (Object)key, (Object)this.bucket.getName());
            final S3Object object = this.s3Service.getObject(this.bucket.getName(), key);
            return object.getDataInputStream();
        }
        catch (ServiceException e) {
            this.handleException((Exception)e, key);
            return null;
        }
    }
    
    @Override
    public InputStream retrieve(final String key, final long byteRangeStart) throws IOException {
        try {
            Jets3tNativeFileSystemStore.LOG.debug("Getting key: {} from bucket: {} with byteRangeStart: {}", new Object[] { key, this.bucket.getName(), byteRangeStart });
            final S3Object object = this.s3Service.getObject(this.bucket, key, (Calendar)null, (Calendar)null, (String[])null, (String[])null, Long.valueOf(byteRangeStart), (Long)null);
            return object.getDataInputStream();
        }
        catch (ServiceException e) {
            this.handleException((Exception)e, key);
            return null;
        }
    }
    
    @Override
    public PartialListing list(final String prefix, final int maxListingLength) throws IOException {
        return this.list(prefix, maxListingLength, null, false);
    }
    
    @Override
    public PartialListing list(final String prefix, final int maxListingLength, final String priorLastKey, final boolean recurse) throws IOException {
        return this.list(prefix, recurse ? null : "/", maxListingLength, priorLastKey);
    }
    
    private PartialListing list(String prefix, final String delimiter, final int maxListingLength, final String priorLastKey) throws IOException {
        try {
            if (!prefix.isEmpty() && !prefix.endsWith("/")) {
                prefix += "/";
            }
            final StorageObjectsChunk chunk = this.s3Service.listObjectsChunked(this.bucket.getName(), prefix, delimiter, (long)maxListingLength, priorLastKey);
            final FileMetadata[] fileMetadata = new FileMetadata[chunk.getObjects().length];
            for (int i = 0; i < fileMetadata.length; ++i) {
                final StorageObject object = chunk.getObjects()[i];
                fileMetadata[i] = new FileMetadata(object.getKey(), object.getContentLength(), object.getLastModifiedDate().getTime());
            }
            return new PartialListing(chunk.getPriorLastKey(), fileMetadata, chunk.getCommonPrefixes());
        }
        catch (ServiceException e) {
            this.handleException((Exception)e, prefix);
            return null;
        }
    }
    
    @Override
    public void delete(final String key) throws IOException {
        try {
            Jets3tNativeFileSystemStore.LOG.debug("Deleting key: {} from bucket: {}", (Object)key, (Object)this.bucket.getName());
            this.s3Service.deleteObject(this.bucket, key);
        }
        catch (ServiceException e) {
            this.handleException((Exception)e, key);
        }
    }
    
    public void rename(final String srcKey, final String dstKey) throws IOException {
        try {
            this.s3Service.renameObject(this.bucket.getName(), srcKey, (StorageObject)new S3Object(dstKey));
        }
        catch (ServiceException e) {
            this.handleException((Exception)e, srcKey);
        }
    }
    
    @Override
    public void copy(final String srcKey, final String dstKey) throws IOException {
        try {
            if (Jets3tNativeFileSystemStore.LOG.isDebugEnabled()) {
                Jets3tNativeFileSystemStore.LOG.debug("Copying srcKey: " + srcKey + "to dstKey: " + dstKey + "in bucket: " + this.bucket.getName());
            }
            if (this.multipartEnabled) {
                final S3Object object = this.s3Service.getObjectDetails(this.bucket, srcKey, (Calendar)null, (Calendar)null, (String[])null, (String[])null);
                if (this.multipartCopyBlockSize > 0L && object.getContentLength() > this.multipartCopyBlockSize) {
                    this.copyLargeFile(object, dstKey);
                    return;
                }
            }
            final S3Object dstObject = new S3Object(dstKey);
            dstObject.setServerSideEncryptionAlgorithm(this.serverSideEncryptionAlgorithm);
            this.s3Service.copyObject(this.bucket.getName(), srcKey, this.bucket.getName(), (StorageObject)dstObject, false);
        }
        catch (ServiceException e) {
            this.handleException((Exception)e, srcKey);
        }
    }
    
    public void copyLargeFile(final S3Object srcObject, final String dstKey) throws IOException {
        try {
            final long partCount = srcObject.getContentLength() / this.multipartCopyBlockSize + ((srcObject.getContentLength() % this.multipartCopyBlockSize > 0L) ? 1 : 0);
            final MultipartUpload multipartUpload = this.s3Service.multipartStartUpload(this.bucket.getName(), dstKey, srcObject.getMetadataMap());
            final List<MultipartPart> listedParts = new ArrayList<MultipartPart>();
            for (int i = 0; i < partCount; ++i) {
                final long byteRangeStart = i * this.multipartCopyBlockSize;
                long byteLength;
                if (i < partCount - 1L) {
                    byteLength = this.multipartCopyBlockSize;
                }
                else {
                    byteLength = srcObject.getContentLength() % this.multipartCopyBlockSize;
                    if (byteLength == 0L) {
                        byteLength = this.multipartCopyBlockSize;
                    }
                }
                final MultipartPart copiedPart = this.s3Service.multipartUploadPartCopy(multipartUpload, Integer.valueOf(i + 1), this.bucket.getName(), srcObject.getKey(), (Calendar)null, (Calendar)null, (String[])null, (String[])null, Long.valueOf(byteRangeStart), Long.valueOf(byteRangeStart + byteLength - 1L), (String)null);
                listedParts.add(copiedPart);
            }
            Collections.reverse(listedParts);
            this.s3Service.multipartCompleteUpload(multipartUpload, (List)listedParts);
        }
        catch (ServiceException e) {
            this.handleException((Exception)e, srcObject.getKey());
        }
    }
    
    @Override
    public void purge(final String prefix) throws IOException {
        String key = "";
        try {
            final S3Object[] arr$;
            final S3Object[] objects = arr$ = this.s3Service.listObjects(this.bucket.getName(), prefix, (String)null);
            for (final S3Object object : arr$) {
                key = object.getKey();
                this.s3Service.deleteObject(this.bucket, key);
            }
        }
        catch (S3ServiceException e) {
            this.handleException((Exception)e, key);
        }
    }
    
    @Override
    public void dump() throws IOException {
        final StringBuilder sb = new StringBuilder("S3 Native Filesystem, ");
        sb.append(this.bucket.getName()).append("\n");
        try {
            final S3Object[] arr$;
            final S3Object[] objects = arr$ = this.s3Service.listObjects(this.bucket.getName());
            for (final S3Object object : arr$) {
                sb.append(object.getKey()).append("\n");
            }
        }
        catch (S3ServiceException e) {
            this.handleException((Exception)e);
        }
        System.out.println(sb);
    }
    
    private void handleException(final Exception e) throws IOException {
        throw this.processException(e, e, "");
    }
    
    private void handleException(final Exception e, final String key) throws IOException {
        throw this.processException(e, e, key);
    }
    
    private IOException processException(final Throwable thrown, final Throwable original, final String key) {
        IOException result = null;
        if (thrown.getCause() != null) {
            result = this.processException(thrown.getCause(), original, key);
        }
        else if (thrown instanceof HttpException) {
            final HttpException httpException = (HttpException)thrown;
            final String responseMessage = httpException.getResponseMessage();
            final int responseCode = httpException.getResponseCode();
            final String bucketName = "s3n://" + this.bucket.getName();
            final String text = String.format("%s : %03d : %s", bucketName, responseCode, responseMessage);
            final String filename = key.isEmpty() ? text : (bucketName + "/" + key);
            switch (responseCode) {
                case 404: {
                    result = new FileNotFoundException(filename);
                    break;
                }
                case 416: {
                    result = new EOFException("Attempted to seek or read past the end of the file: " + filename);
                    break;
                }
                case 403: {
                    result = (IOException)new AccessControlException("Permission denied: " + filename);
                    break;
                }
                default: {
                    result = new IOException(text);
                    break;
                }
            }
            result.initCause(thrown);
        }
        else if (thrown instanceof S3ServiceException) {
            final S3ServiceException se = (S3ServiceException)thrown;
            Jets3tNativeFileSystemStore.LOG.debug("S3ServiceException: {}: {} : {}", new Object[] { se.getS3ErrorCode(), se.getS3ErrorMessage(), se, se });
            if ("InvalidRange".equals(se.getS3ErrorCode())) {
                result = new EOFException("Attempted to seek or read past the end of the file");
            }
            else {
                result = new S3Exception((Throwable)se);
            }
        }
        else if (thrown instanceof ServiceException) {
            final ServiceException se2 = (ServiceException)thrown;
            Jets3tNativeFileSystemStore.LOG.debug("S3ServiceException: {}: {} : {}", new Object[] { se2.getErrorCode(), se2.toString(), se2, se2 });
            result = new S3Exception((Throwable)se2);
        }
        else if (thrown instanceof IOException) {
            result = (IOException)thrown;
        }
        else {
            result = new S3Exception(original);
        }
        return result;
    }
    
    static {
        LOG = LoggerFactory.getLogger((Class)Jets3tNativeFileSystemStore.class);
    }
}
