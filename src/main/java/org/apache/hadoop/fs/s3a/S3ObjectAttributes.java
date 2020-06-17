// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3a;

class S3ObjectAttributes
{
    private String bucket;
    private String key;
    private S3AEncryptionMethods serverSideEncryptionAlgorithm;
    private String serverSideEncryptionKey;
    
    public S3ObjectAttributes(final String bucket, final String key, final S3AEncryptionMethods serverSideEncryptionAlgorithm, final String serverSideEncryptionKey) {
        this.bucket = bucket;
        this.key = key;
        this.serverSideEncryptionAlgorithm = serverSideEncryptionAlgorithm;
        this.serverSideEncryptionKey = serverSideEncryptionKey;
    }
    
    public String getBucket() {
        return this.bucket;
    }
    
    public String getKey() {
        return this.key;
    }
    
    public S3AEncryptionMethods getServerSideEncryptionAlgorithm() {
        return this.serverSideEncryptionAlgorithm;
    }
    
    public String getServerSideEncryptionKey() {
        return this.serverSideEncryptionKey;
    }
}
