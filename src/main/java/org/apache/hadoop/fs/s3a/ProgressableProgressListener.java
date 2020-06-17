// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3a;

import com.amazonaws.event.ProgressEventType;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.services.s3.transfer.Upload;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import com.amazonaws.event.ProgressListener;

public class ProgressableProgressListener implements ProgressListener
{
    private static final Logger LOG;
    private final S3AFileSystem fs;
    private final String key;
    private final Progressable progress;
    private long lastBytesTransferred;
    private final Upload upload;
    
    public ProgressableProgressListener(final S3AFileSystem fs, final String key, final Upload upload, final Progressable progress) {
        this.fs = fs;
        this.key = key;
        this.upload = upload;
        this.progress = progress;
        this.lastBytesTransferred = 0L;
    }
    
    public void progressChanged(final ProgressEvent progressEvent) {
        if (this.progress != null) {
            this.progress.progress();
        }
        final ProgressEventType pet = progressEvent.getEventType();
        if (pet == ProgressEventType.TRANSFER_PART_STARTED_EVENT || pet == ProgressEventType.TRANSFER_COMPLETED_EVENT) {
            this.fs.incrementWriteOperations();
        }
        final long transferred = this.upload.getProgress().getBytesTransferred();
        final long delta = transferred - this.lastBytesTransferred;
        this.fs.incrementPutProgressStatistics(this.key, delta);
        this.lastBytesTransferred = transferred;
    }
    
    public long uploadCompleted() {
        final long delta = this.upload.getProgress().getBytesTransferred() - this.lastBytesTransferred;
        if (delta > 0L) {
            ProgressableProgressListener.LOG.debug("S3A write delta changed after finished: {} bytes", (Object)delta);
            this.fs.incrementPutProgressStatistics(this.key, delta);
        }
        return delta;
    }
    
    static {
        LOG = S3AFileSystem.LOG;
    }
}
