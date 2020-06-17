// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3a;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.AmazonClientException;
import java.io.InterruptedIOException;
import com.amazonaws.event.ProgressListener;
import java.io.IOException;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.apache.hadoop.util.Progressable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.io.File;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceAudience;
import java.io.OutputStream;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class S3AOutputStream extends OutputStream
{
    private final OutputStream backupStream;
    private final File backupFile;
    private final AtomicBoolean closed;
    private final String key;
    private final Progressable progress;
    private final S3AFileSystem fs;
    public static final Logger LOG;
    
    public S3AOutputStream(final Configuration conf, final S3AFileSystem fs, final String key, final Progressable progress) throws IOException {
        this.closed = new AtomicBoolean(false);
        this.key = key;
        this.progress = progress;
        this.fs = fs;
        this.backupFile = fs.createTmpFileForWrite("output-", -1L, conf);
        S3AOutputStream.LOG.debug("OutputStream for key '{}' writing to tempfile: {}", (Object)key, (Object)this.backupFile);
        this.backupStream = new BufferedOutputStream(new FileOutputStream(this.backupFile));
    }
    
    void checkOpen() throws IOException {
        if (this.closed.get()) {
            throw new IOException("Output Stream closed");
        }
    }
    
    @Override
    public void flush() throws IOException {
        this.checkOpen();
        this.backupStream.flush();
    }
    
    @Override
    public void close() throws IOException {
        if (this.closed.getAndSet(true)) {
            return;
        }
        this.backupStream.close();
        S3AOutputStream.LOG.debug("OutputStream for key '{}' closed. Now beginning upload", (Object)this.key);
        try {
            final ObjectMetadata om = this.fs.newObjectMetadata(this.backupFile.length());
            final UploadInfo info = this.fs.putObject(this.fs.newPutObjectRequest(this.key, om, this.backupFile));
            final ProgressableProgressListener listener = new ProgressableProgressListener(this.fs, this.key, info.getUpload(), this.progress);
            info.getUpload().addProgressListener((ProgressListener)listener);
            info.getUpload().waitForUploadResult();
            listener.uploadCompleted();
            this.fs.finishedWrite(this.key, info.getLength());
        }
        catch (InterruptedException e) {
            throw (InterruptedIOException)new InterruptedIOException(e.toString()).initCause(e);
        }
        catch (AmazonClientException e2) {
            throw S3AUtils.translateException("saving output", this.key, e2);
        }
        finally {
            if (!this.backupFile.delete()) {
                S3AOutputStream.LOG.warn("Could not delete temporary s3a file: {}", (Object)this.backupFile);
            }
            super.close();
        }
        S3AOutputStream.LOG.debug("OutputStream for key '{}' upload complete", (Object)this.key);
    }
    
    @Override
    public void write(final int b) throws IOException {
        this.checkOpen();
        this.backupStream.write(b);
    }
    
    @Override
    public void write(final byte[] b, final int off, final int len) throws IOException {
        this.checkOpen();
        this.backupStream.write(b, off, len);
    }
    
    static {
        LOG = S3AFileSystem.LOG;
    }
}
