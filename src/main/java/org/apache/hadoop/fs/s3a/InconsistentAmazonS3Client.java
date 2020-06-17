// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3a;

import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import java.util.List;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import java.util.HashMap;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import java.util.Map;
import org.slf4j.Logger;
import com.amazonaws.services.s3.AmazonS3Client;

public class InconsistentAmazonS3Client extends AmazonS3Client
{
    public static final String DELAY_KEY_SUBSTRING = "DELAY_LISTING_ME";
    public static final long DELAY_KEY_MILLIS = 5000L;
    private static final Logger LOG;
    private Map<String, Delete> delayedDeletes;
    private Map<String, Long> delayedPutKeys;
    
    public InconsistentAmazonS3Client(final AWSCredentialsProvider credentials, final ClientConfiguration clientConfiguration) {
        super(credentials, clientConfiguration);
        this.delayedDeletes = new HashMap<String, Delete>();
        this.delayedPutKeys = new HashMap<String, Long>();
    }
    
    public void deleteObject(final DeleteObjectRequest deleteObjectRequest) throws AmazonClientException, AmazonServiceException {
        InconsistentAmazonS3Client.LOG.debug("key {}", (Object)deleteObjectRequest.getKey());
        this.registerDeleteObject(deleteObjectRequest);
        super.deleteObject(deleteObjectRequest);
    }
    
    public PutObjectResult putObject(final PutObjectRequest putObjectRequest) throws AmazonClientException, AmazonServiceException {
        InconsistentAmazonS3Client.LOG.debug("key {}", (Object)putObjectRequest.getKey());
        this.registerPutObject(putObjectRequest);
        return super.putObject(putObjectRequest);
    }
    
    public ObjectListing listObjects(final ListObjectsRequest listObjectsRequest) throws AmazonClientException, AmazonServiceException {
        InconsistentAmazonS3Client.LOG.debug("prefix {}", (Object)listObjectsRequest.getPrefix());
        ObjectListing listing = super.listObjects(listObjectsRequest);
        listing = this.filterListObjects(listObjectsRequest, listing);
        listing = this.restoreListObjects(listObjectsRequest, listing);
        return listing;
    }
    
    private boolean addIfNotPresent(final List<S3ObjectSummary> list, final S3ObjectSummary item) {
        final String key = item.getKey();
        for (final S3ObjectSummary member : list) {
            if (member.getKey().equals(key)) {
                return false;
            }
        }
        return list.add(item);
    }
    
    private ObjectListing restoreListObjects(final ListObjectsRequest request, final ObjectListing rawListing) {
        final List<S3ObjectSummary> outputList = (List<S3ObjectSummary>)rawListing.getObjectSummaries();
        final List<String> outputPrefixes = (List<String>)rawListing.getCommonPrefixes();
        for (final String key : this.delayedDeletes.keySet()) {
            final Delete delete = this.delayedDeletes.get(key);
            if (this.isKeyDelayed(delete.time(), key)) {
                if (!key.startsWith(request.getPrefix())) {
                    continue;
                }
                if (delete.summary == null) {
                    if (outputPrefixes.contains(key)) {
                        continue;
                    }
                    outputPrefixes.add(key);
                }
                else {
                    this.addIfNotPresent(outputList, delete.summary());
                }
            }
            else {
                this.delayedDeletes.remove(key);
            }
        }
        return new CustomObjectListing(rawListing, outputList, outputPrefixes);
    }
    
    private ObjectListing filterListObjects(final ListObjectsRequest request, final ObjectListing rawListing) {
        final List<S3ObjectSummary> outputList = new ArrayList<S3ObjectSummary>();
        for (final S3ObjectSummary s : rawListing.getObjectSummaries()) {
            final String key = s.getKey();
            if (!this.isKeyDelayed(this.delayedPutKeys.get(key), key)) {
                outputList.add(s);
            }
        }
        final List<String> outputPrefixes = new ArrayList<String>();
        final Iterator i$2 = rawListing.getCommonPrefixes().iterator();
        while (i$2.hasNext()) {
            final String key = (String) i$2.next();
            if (!this.isKeyDelayed(this.delayedPutKeys.get(key), key)) {
                outputPrefixes.add(key);
            }
        }
        return new CustomObjectListing(rawListing, outputList, outputPrefixes);
    }
    
    private boolean isKeyDelayed(final Long enqueueTime, final String key) {
        if (enqueueTime == null) {
            InconsistentAmazonS3Client.LOG.debug("no delay for key {}", (Object)key);
            return false;
        }
        final long currentTime = System.currentTimeMillis();
        final long deadline = enqueueTime + 5000L;
        if (currentTime >= deadline) {
            this.delayedDeletes.remove(key);
            InconsistentAmazonS3Client.LOG.debug("no longer delaying {}", (Object)key);
            return false;
        }
        InconsistentAmazonS3Client.LOG.info("delaying {}", (Object)key);
        return true;
    }
    
    private void registerDeleteObject(final DeleteObjectRequest req) {
        final String key = req.getKey();
        if (this.shouldDelay(key)) {
            S3ObjectSummary summary = null;
            final ObjectListing list = this.listObjects(req.getBucketName(), key);
            for (final S3ObjectSummary result : list.getObjectSummaries()) {
                if (result.getKey().equals(key)) {
                    summary = result;
                    break;
                }
            }
            this.delayedDeletes.put(key, new Delete(System.currentTimeMillis(), summary));
        }
    }
    
    private void registerPutObject(final PutObjectRequest req) {
        final String key = req.getKey();
        if (this.shouldDelay(key)) {
            this.enqueueDelayedPut(key);
        }
    }
    
    private boolean shouldDelay(final String key) {
        final boolean delay = key.contains("DELAY_LISTING_ME");
        InconsistentAmazonS3Client.LOG.debug("{} -> {}", (Object)key, (Object)delay);
        return delay;
    }
    
    private void enqueueDelayedPut(final String key) {
        InconsistentAmazonS3Client.LOG.debug("delaying put of {}", (Object)key);
        this.delayedPutKeys.put(key, System.currentTimeMillis());
    }
    
    static {
        LOG = LoggerFactory.getLogger((Class)InconsistentAmazonS3Client.class);
    }
    
    private static class Delete
    {
        private Long time;
        private S3ObjectSummary summary;
        
        Delete(final Long time, final S3ObjectSummary summary) {
            this.time = time;
            this.summary = summary;
        }
        
        public Long time() {
            return this.time;
        }
        
        public S3ObjectSummary summary() {
            return this.summary;
        }
    }
    
    private static class CustomObjectListing extends ObjectListing
    {
        private final List<S3ObjectSummary> customListing;
        private final List<String> customPrefixes;
        
        public CustomObjectListing(final ObjectListing rawListing, final List<S3ObjectSummary> customListing, final List<String> customPrefixes) {
            this.customListing = customListing;
            this.customPrefixes = customPrefixes;
            this.setBucketName(rawListing.getBucketName());
            this.setCommonPrefixes(rawListing.getCommonPrefixes());
            this.setDelimiter(rawListing.getDelimiter());
            this.setEncodingType(rawListing.getEncodingType());
            this.setMarker(rawListing.getMarker());
            this.setMaxKeys(rawListing.getMaxKeys());
            this.setNextMarker(rawListing.getNextMarker());
            this.setPrefix(rawListing.getPrefix());
            this.setTruncated(rawListing.isTruncated());
        }
        
        public List<S3ObjectSummary> getObjectSummaries() {
            return this.customListing;
        }
        
        public List<String> getCommonPrefixes() {
            return this.customPrefixes;
        }
    }
}
