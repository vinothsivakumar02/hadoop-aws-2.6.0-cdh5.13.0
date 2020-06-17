// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3a;

import com.amazonaws.services.s3.transfer.Upload;

public class UploadInfo
{
    private Upload upload;
    private long length;
    
    public UploadInfo(final Upload upload, final long length) {
        this.upload = upload;
        this.length = length;
    }
    
    public Upload getUpload() {
        return this.upload;
    }
    
    public long getLength() {
        return this.length;
    }
}
