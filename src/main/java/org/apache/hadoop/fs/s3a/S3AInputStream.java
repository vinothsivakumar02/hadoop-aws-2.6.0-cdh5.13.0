// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3a;

import java.io.EOFException;
import com.amazonaws.AmazonClientException;
import java.io.IOException;
import com.amazonaws.services.s3.model.SSECustomerKey;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import com.amazonaws.services.s3.AmazonS3;
import org.apache.hadoop.fs.FileSystem;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.CanSetReadahead;
import org.apache.hadoop.fs.FSInputStream;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class S3AInputStream extends FSInputStream implements CanSetReadahead
{
    private long pos;
    private volatile boolean closed;
    private S3ObjectInputStream wrappedStream;
    private final FileSystem.Statistics stats;
    private final AmazonS3 client;
    private final String bucket;
    private final String key;
    private final long contentLength;
    private final String uri;
    public static final Logger LOG;
    private final S3AInstrumentation.InputStreamStatistics streamStatistics;
    private S3AEncryptionMethods serverSideEncryptionAlgorithm;
    private String serverSideEncryptionKey;
    private final S3AInputPolicy inputPolicy;
    private long readahead;
    private long nextReadPos;
    private long contentRangeFinish;
    private long contentRangeStart;
    
    public S3AInputStream(final S3ObjectAttributes s3Attributes, final long contentLength, final AmazonS3 client, final FileSystem.Statistics stats, final S3AInstrumentation instrumentation, final long readahead, final S3AInputPolicy inputPolicy) {
        this.readahead = 65536L;
        Preconditions.checkArgument(StringUtils.isNotEmpty(s3Attributes.getBucket()), (Object)"No Bucket");
        Preconditions.checkArgument(StringUtils.isNotEmpty(s3Attributes.getKey()), (Object)"No Key");
        Preconditions.checkArgument(contentLength >= 0L, (Object)"Negative content length");
        this.bucket = s3Attributes.getBucket();
        this.key = s3Attributes.getKey();
        this.contentLength = contentLength;
        this.client = client;
        this.stats = stats;
        this.uri = "s3a://" + this.bucket + "/" + this.key;
        this.streamStatistics = instrumentation.newInputStreamStatistics();
        this.serverSideEncryptionAlgorithm = s3Attributes.getServerSideEncryptionAlgorithm();
        this.serverSideEncryptionKey = s3Attributes.getServerSideEncryptionKey();
        this.inputPolicy = inputPolicy;
        this.setReadahead(readahead);
    }
    
    private synchronized void reopen(final String reason, final long targetPos, final long length) throws IOException {
        if (this.wrappedStream != null) {
            this.closeStream("reopen(" + reason + ")", this.contentRangeFinish, false);
        }
        this.contentRangeFinish = calculateRequestLimit(this.inputPolicy, targetPos, length, this.contentLength, this.readahead);
        S3AInputStream.LOG.debug("reopen({}) for {} range[{}-{}], length={}, streamPosition={}, nextReadPosition={}", new Object[] { this.uri, reason, targetPos, this.contentRangeFinish, length, this.pos, this.nextReadPos });
        this.streamStatistics.streamOpened();
        try {
            final GetObjectRequest request = new GetObjectRequest(this.bucket, this.key).withRange(targetPos, this.contentRangeFinish);
            if (S3AEncryptionMethods.SSE_C.equals(this.serverSideEncryptionAlgorithm) && StringUtils.isNotBlank(this.serverSideEncryptionKey)) {
                request.setSSECustomerKey(new SSECustomerKey(this.serverSideEncryptionKey));
            }
            this.wrappedStream = this.client.getObject(request).getObjectContent();
            this.contentRangeStart = targetPos;
            if (this.wrappedStream == null) {
                throw new IOException("Null IO stream from reopen of (" + reason + ") " + this.uri);
            }
        }
        catch (AmazonClientException e) {
            throw S3AUtils.translateException("Reopen at position " + targetPos, this.uri, e);
        }
        this.pos = targetPos;
    }
    
    public synchronized long getPos() throws IOException {
        return (this.nextReadPos < 0L) ? 0L : this.nextReadPos;
    }
    
    public synchronized void seek(final long targetPos) throws IOException {
        this.checkNotClosed();
        if (targetPos < 0L) {
            throw new EOFException("Cannot seek to a negative offset " + targetPos);
        }
        if (this.contentLength <= 0L) {
            return;
        }
        this.nextReadPos = targetPos;
    }
    
    private void seekQuietly(final long positiveTargetPos) {
        try {
            this.seek(positiveTargetPos);
        }
        catch (IOException ioe) {
            S3AInputStream.LOG.debug("Ignoring IOE on seek of {} to {}", new Object[] { this.uri, positiveTargetPos, ioe });
        }
    }
    
    private void seekInStream(final long targetPos, final long length) throws IOException {
        this.checkNotClosed();
        if (this.wrappedStream == null) {
            return;
        }
        final long diff = targetPos - this.pos;
        if (diff > 0L) {
            final int available = this.wrappedStream.available();
            final long forwardSeekRange = Math.max(this.readahead, available);
            final long remainingInCurrentRequest = this.remainingInCurrentRequest();
            final long forwardSeekLimit = Math.min(remainingInCurrentRequest, forwardSeekRange);
            final boolean skipForward = remainingInCurrentRequest > 0L && diff <= forwardSeekLimit;
            if (skipForward) {
                S3AInputStream.LOG.debug("Forward seek on {}, of {} bytes", (Object)this.uri, (Object)diff);
                this.streamStatistics.seekForwards(diff);
                final long skipped = this.wrappedStream.skip(diff);
                if (skipped > 0L) {
                    this.pos += skipped;
                    this.incrementBytesRead(diff);
                }
                if (this.pos == targetPos) {
                    return;
                }
                S3AInputStream.LOG.warn("Failed to seek on {} to {}. Current position {}", new Object[] { this.uri, targetPos, this.pos });
            }
        }
        else if (diff < 0L) {
            this.streamStatistics.seekBackwards(diff);
        }
        else if (this.remainingInCurrentRequest() > 0L) {
            return;
        }
        this.closeStream("seekInStream()", this.contentRangeFinish, false);
        this.pos = targetPos;
    }
    
    public boolean seekToNewSource(final long targetPos) throws IOException {
        return false;
    }
    
    private void lazySeek(final long targetPos, final long len) throws IOException {
        this.seekInStream(targetPos, len);
        if (this.wrappedStream == null) {
            this.reopen("read from new offset", targetPos, len);
        }
    }
    
    private void incrementBytesRead(final long bytesRead) {
        this.streamStatistics.bytesRead(bytesRead);
        if (this.stats != null && bytesRead > 0L) {
            this.stats.incrementBytesRead(bytesRead);
        }
    }
    
    public synchronized int read() throws IOException {
        this.checkNotClosed();
        if (this.contentLength == 0L || this.nextReadPos >= this.contentLength) {
            return -1;
        }
        int byteRead;
        try {
            this.lazySeek(this.nextReadPos, 1L);
            byteRead = this.wrappedStream.read();
        }
        catch (EOFException e2) {
            return -1;
        }
        catch (IOException e) {
            this.onReadFailure(e, 1);
            byteRead = this.wrappedStream.read();
        }
        if (byteRead >= 0) {
            ++this.pos;
            ++this.nextReadPos;
        }
        if (byteRead >= 0) {
            this.incrementBytesRead(1L);
        }
        return byteRead;
    }
    
    private void onReadFailure(final IOException ioe, final int length) throws IOException {
        S3AInputStream.LOG.info("Got exception while trying to read from stream {} trying to recover: " + ioe, (Object)this.uri);
        S3AInputStream.LOG.debug("While trying to read from stream {}", (Object)this.uri, (Object)ioe);
        this.streamStatistics.readException();
        this.reopen("failure recovery", this.pos, length);
    }
    
    public synchronized int read(final byte[] buf, final int off, final int len) throws IOException {
        this.checkNotClosed();
        this.validatePositionedReadArgs(this.nextReadPos, buf, off, len);
        if (len == 0) {
            return 0;
        }
        if (this.contentLength == 0L || this.nextReadPos >= this.contentLength) {
            return -1;
        }
        try {
            this.lazySeek(this.nextReadPos, len);
        }
        catch (EOFException e3) {
            return -1;
        }
        int bytesRead;
        try {
            this.streamStatistics.readOperationStarted(this.nextReadPos, len);
            bytesRead = this.wrappedStream.read(buf, off, len);
        }
        catch (EOFException e) {
            this.onReadFailure(e, len);
            return -1;
        }
        catch (IOException e2) {
            this.onReadFailure(e2, len);
            bytesRead = this.wrappedStream.read(buf, off, len);
        }
        if (bytesRead > 0) {
            this.pos += bytesRead;
            this.nextReadPos += bytesRead;
        }
        this.incrementBytesRead(bytesRead);
        this.streamStatistics.readOperationCompleted(len, bytesRead);
        return bytesRead;
    }
    
    private void checkNotClosed() throws IOException {
        if (this.closed) {
            throw new IOException(this.uri + ": " + "Stream is closed!");
        }
    }
    
    public synchronized void close() throws IOException {
        if (!this.closed) {
            this.closed = true;
            try {
                this.closeStream("close() operation", this.contentRangeFinish, false);
                super.close();
            }
            finally {
                this.streamStatistics.close();
            }
        }
    }
    
    private void closeStream(final String reason, final long length, final boolean forceAbort) {
        if (this.wrappedStream != null) {
            final long remaining = this.remainingInCurrentRequest();
            boolean shouldAbort = forceAbort || remaining > this.readahead;
            if (!shouldAbort) {
                try {
                    this.wrappedStream.close();
                    this.streamStatistics.streamClose(false, remaining);
                }
                catch (IOException e) {
                    S3AInputStream.LOG.debug("When closing {} stream for {}", new Object[] { this.uri, reason, e });
                    shouldAbort = true;
                }
            }
            if (shouldAbort) {
                this.wrappedStream.abort();
                this.streamStatistics.streamClose(true, remaining);
            }
            S3AInputStream.LOG.debug("Stream {} {}: {}; streamPos={}, nextReadPos={}, request range {}-{} length={}", new Object[] { this.uri, shouldAbort ? "aborted" : "closed", reason, this.pos, this.nextReadPos, this.contentRangeStart, this.contentRangeFinish, length });
            this.wrappedStream = null;
        }
    }
    
    @InterfaceStability.Unstable
    public synchronized boolean resetConnection() throws IOException {
        this.checkNotClosed();
        final boolean connectionOpen = this.wrappedStream != null;
        if (connectionOpen) {
            S3AInputStream.LOG.info("Forced reset of connection to {}", (Object)this.uri);
            this.closeStream("reset()", this.contentRangeFinish, true);
        }
        return connectionOpen;
    }
    
    public synchronized int available() throws IOException {
        this.checkNotClosed();
        final long remaining = this.remainingInFile();
        if (remaining > 2147483647L) {
            return Integer.MAX_VALUE;
        }
        return (int)remaining;
    }
    
    @InterfaceAudience.Private
    @InterfaceStability.Unstable
    public synchronized long remainingInFile() {
        return this.contentLength - this.pos;
    }
    
    @InterfaceAudience.Private
    @InterfaceStability.Unstable
    public synchronized long remainingInCurrentRequest() {
        return this.contentRangeFinish - this.pos;
    }
    
    @InterfaceAudience.Private
    @InterfaceStability.Unstable
    public synchronized long getContentRangeFinish() {
        return this.contentRangeFinish;
    }
    
    @InterfaceAudience.Private
    @InterfaceStability.Unstable
    public synchronized long getContentRangeStart() {
        return this.contentRangeStart;
    }
    
    public boolean markSupported() {
        return false;
    }
    
    @InterfaceStability.Unstable
    public String toString() {
        final String s = this.streamStatistics.toString();
        synchronized (this) {
            final StringBuilder sb = new StringBuilder("S3AInputStream{");
            sb.append(this.uri);
            sb.append(" wrappedStream=").append((this.wrappedStream != null) ? "open" : "closed");
            sb.append(" read policy=").append(this.inputPolicy);
            sb.append(" pos=").append(this.pos);
            sb.append(" nextReadPos=").append(this.nextReadPos);
            sb.append(" contentLength=").append(this.contentLength);
            sb.append(" contentRangeStart=").append(this.contentRangeStart);
            sb.append(" contentRangeFinish=").append(this.contentRangeFinish);
            sb.append(" remainingInCurrentRequest=").append(this.remainingInCurrentRequest());
            sb.append('\n').append(s);
            sb.append('}');
            return sb.toString();
        }
    }
    
    public void readFully(final long position, final byte[] buffer, final int offset, final int length) throws IOException {
        this.checkNotClosed();
        this.validatePositionedReadArgs(position, buffer, offset, length);
        this.streamStatistics.readFullyOperationStarted(position, length);
        if (length == 0) {
            return;
        }
        int nread = 0;
        synchronized (this) {
            final long oldPos = this.getPos();
            try {
                this.seek(position);
                while (nread < length) {
                    final int nbytes = this.read(buffer, offset + nread, length - nread);
                    if (nbytes < 0) {
                        throw new EOFException("End of file reached before reading fully.");
                    }
                    nread += nbytes;
                }
            }
            finally {
                this.seekQuietly(oldPos);
            }
        }
    }
    
    @InterfaceAudience.Private
    @InterfaceStability.Unstable
    public S3AInstrumentation.InputStreamStatistics getS3AStreamStatistics() {
        return this.streamStatistics;
    }
    
    public synchronized void setReadahead(final Long readahead) {
        if (readahead == null) {
            this.readahead = 65536L;
        }
        else {
            Preconditions.checkArgument(readahead >= 0L, (Object)"Negative readahead value");
            this.readahead = readahead;
        }
    }
    
    public synchronized long getReadahead() {
        return this.readahead;
    }
    
    static long calculateRequestLimit(final S3AInputPolicy inputPolicy, final long targetPos, final long length, final long contentLength, final long readahead) {
        long rangeLimit = 0L;
        switch (inputPolicy) {
            case Random: {
                rangeLimit = ((length < 0L) ? contentLength : (targetPos + Math.max(readahead, length)));
                break;
            }
            case Sequential: {
                rangeLimit = contentLength;
                break;
            }
            default: {
                rangeLimit = contentLength;
                break;
            }
        }
        rangeLimit = Math.min(contentLength, rangeLimit);
        return rangeLimit;
    }
    
    static {
        LOG = S3AFileSystem.LOG;
    }
}
