// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3a;

import com.amazonaws.event.ProgressEventType;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import java.util.Iterator;
import com.google.common.util.concurrent.Futures;
import com.amazonaws.services.s3.model.UploadPartRequest;
import java.util.ArrayList;
import org.slf4j.LoggerFactory;
import com.google.common.util.concurrent.ListenableFuture;
import java.util.concurrent.ExecutionException;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import java.util.concurrent.Callable;
import com.amazonaws.services.s3.model.PartETag;
import java.io.Closeable;
import java.util.List;
import java.io.IOException;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.base.Preconditions;
import org.apache.hadoop.io.retry.RetryPolicies;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.util.Progressable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.io.retry.RetryPolicy;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.amazonaws.event.ProgressListener;
import org.slf4j.Logger;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceAudience;
import java.io.OutputStream;

@InterfaceAudience.Private
@InterfaceStability.Unstable
class S3ABlockOutputStream extends OutputStream
{
    private static final Logger LOG;
    private final S3AFileSystem fs;
    private final String key;
    private final int blockSize;
    private long bytesSubmitted;
    private final ProgressListener progressListener;
    private final ListeningExecutorService executorService;
    private final RetryPolicy retryPolicy;
    private final S3ADataBlocks.BlockFactory blockFactory;
    private final byte[] singleCharWrite;
    private MultiPartUpload multiPartUpload;
    private final AtomicBoolean closed;
    private S3ADataBlocks.DataBlock activeBlock;
    private long blockCount;
    private final S3AInstrumentation.OutputStreamStatistics statistics;
    private final S3AFileSystem.WriteOperationHelper writeOperationHelper;
    
    S3ABlockOutputStream(final S3AFileSystem fs, final String key, final ExecutorService executorService, final Progressable progress, final long blockSize, final S3ADataBlocks.BlockFactory blockFactory, final S3AInstrumentation.OutputStreamStatistics statistics, final S3AFileSystem.WriteOperationHelper writeOperationHelper) throws IOException {
        this.retryPolicy = RetryPolicies.retryUpToMaximumCountWithProportionalSleep(5, 2000L, TimeUnit.MILLISECONDS);
        this.singleCharWrite = new byte[1];
        this.closed = new AtomicBoolean(false);
        this.blockCount = 0L;
        this.fs = fs;
        this.key = key;
        this.blockFactory = blockFactory;
        this.blockSize = (int)blockSize;
        this.statistics = statistics;
        this.writeOperationHelper = writeOperationHelper;
        Preconditions.checkArgument(blockSize >= 5242880L, "Block size is too small: %d", new Object[] { blockSize });
        this.executorService = MoreExecutors.listeningDecorator(executorService);
        this.multiPartUpload = null;
        this.progressListener = (ProgressListener)((progress instanceof ProgressListener) ? ((ProgressListener)progress) : new ProgressableListener(progress));
        this.createBlockIfNeeded();
        S3ABlockOutputStream.LOG.debug("Initialized S3ABlockOutputStream for {} output to {}", (Object)writeOperationHelper, (Object)this.activeBlock);
    }
    
    private synchronized S3ADataBlocks.DataBlock createBlockIfNeeded() throws IOException {
        if (this.activeBlock == null) {
            ++this.blockCount;
            if (this.blockCount >= 10000L) {
                S3ABlockOutputStream.LOG.error("Number of partitions in stream exceeds limit for S3: 10000 write may fail.");
            }
            this.activeBlock = this.blockFactory.create(this.blockCount, this.blockSize, this.statistics);
        }
        return this.activeBlock;
    }
    
    private synchronized S3ADataBlocks.DataBlock getActiveBlock() {
        return this.activeBlock;
    }
    
    private synchronized boolean hasActiveBlock() {
        return this.activeBlock != null;
    }
    
    private void clearActiveBlock() {
        if (this.activeBlock != null) {
            S3ABlockOutputStream.LOG.debug("Clearing active block");
        }
        synchronized (this) {
            this.activeBlock = null;
        }
    }
    
    void checkOpen() throws IOException {
        if (this.closed.get()) {
            throw new IOException("Filesystem " + this.writeOperationHelper + " closed");
        }
    }
    
    @Override
    public synchronized void flush() throws IOException {
        this.checkOpen();
        final S3ADataBlocks.DataBlock dataBlock = this.getActiveBlock();
        if (dataBlock != null) {
            dataBlock.flush();
        }
    }
    
    @Override
    public synchronized void write(final int b) throws IOException {
        this.singleCharWrite[0] = (byte)b;
        this.write(this.singleCharWrite, 0, 1);
    }
    
    @Override
    public synchronized void write(final byte[] source, final int offset, final int len) throws IOException {
        S3ADataBlocks.validateWriteArgs(source, offset, len);
        this.checkOpen();
        if (len == 0) {
            return;
        }
        final S3ADataBlocks.DataBlock block = this.createBlockIfNeeded();
        final int written = block.write(source, offset, len);
        final int remainingCapacity = block.remainingCapacity();
        if (written < len) {
            S3ABlockOutputStream.LOG.debug("writing more data than block has capacity -triggering upload");
            this.uploadCurrentBlock();
            this.write(source, offset + written, len - written);
        }
        else if (remainingCapacity == 0) {
            this.uploadCurrentBlock();
        }
    }
    
    private synchronized void uploadCurrentBlock() throws IOException {
        Preconditions.checkState(this.hasActiveBlock(), (Object)"No active block");
        S3ABlockOutputStream.LOG.debug("Writing block # {}", (Object)this.blockCount);
        if (this.multiPartUpload == null) {
            S3ABlockOutputStream.LOG.debug("Initiating Multipart upload");
            this.multiPartUpload = new MultiPartUpload();
        }
        try {
            this.multiPartUpload.uploadBlockAsync(this.getActiveBlock());
            this.bytesSubmitted += this.getActiveBlock().dataSize();
        }
        finally {
            this.clearActiveBlock();
        }
    }
    
    @Override
    public void close() throws IOException {
        if (this.closed.getAndSet(true)) {
            S3ABlockOutputStream.LOG.debug("Ignoring close() as stream is already closed");
            return;
        }
        final S3ADataBlocks.DataBlock block = this.getActiveBlock();
        final boolean hasBlock = this.hasActiveBlock();
        S3ABlockOutputStream.LOG.debug("{}: Closing block #{}: current block= {}", new Object[] { this, this.blockCount, hasBlock ? block : "(none)" });
        long bytes = 0L;
        try {
            if (this.multiPartUpload == null) {
                if (hasBlock) {
                    bytes = this.putObject();
                }
            }
            else {
                if (hasBlock && block.hasData()) {
                    this.uploadCurrentBlock();
                }
                final List<PartETag> partETags = this.multiPartUpload.waitForAllPartUploads();
                this.multiPartUpload.complete(partETags);
                bytes = this.bytesSubmitted;
            }
            S3ABlockOutputStream.LOG.debug("Upload complete for {}", (Object)this.writeOperationHelper);
        }
        catch (IOException ioe) {
            this.writeOperationHelper.writeFailed(ioe);
            throw ioe;
        }
        finally {
            S3AUtils.closeAll(S3ABlockOutputStream.LOG, block, this.blockFactory);
            S3ABlockOutputStream.LOG.debug("Statistics: {}", (Object)this.statistics);
            S3AUtils.closeAll(S3ABlockOutputStream.LOG, this.statistics);
            this.clearActiveBlock();
        }
        this.writeOperationHelper.writeSuccessful(bytes);
    }
    
    private int putObject() throws IOException {
        S3ABlockOutputStream.LOG.debug("Executing regular upload for {}", (Object)this.writeOperationHelper);
        final S3ADataBlocks.DataBlock block = this.getActiveBlock();
        final int size = block.dataSize();
        final S3ADataBlocks.BlockUploadData uploadData = block.startUpload();
        final PutObjectRequest putObjectRequest = uploadData.hasFile() ? this.writeOperationHelper.newPutRequest(uploadData.getFile()) : this.writeOperationHelper.newPutRequest(uploadData.getUploadStream(), size);
        final long transferQueueTime = this.now();
        final BlockUploadProgress callback = new BlockUploadProgress(block, this.progressListener, transferQueueTime);
        putObjectRequest.setGeneralProgressListener((ProgressListener)callback);
        this.statistics.blockUploadQueued(size);
        final ListenableFuture<PutObjectResult> putObjectResult = (ListenableFuture<PutObjectResult>)this.executorService.submit((Callable)new Callable<PutObjectResult>() {
            @Override
            public PutObjectResult call() throws Exception {
                PutObjectResult result;
                try {
                    result = S3ABlockOutputStream.this.writeOperationHelper.putObject(putObjectRequest);
                }
                finally {
                    S3AUtils.closeAll(S3ABlockOutputStream.LOG, uploadData, block);
                }
                return result;
            }
        });
        this.clearActiveBlock();
        try {
            putObjectResult.get();
            return size;
        }
        catch (InterruptedException ie) {
            S3ABlockOutputStream.LOG.warn("Interrupted object upload", (Throwable)ie);
            Thread.currentThread().interrupt();
            return 0;
        }
        catch (ExecutionException ee) {
            throw S3AUtils.extractException("regular upload", this.key, ee);
        }
    }
    
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("S3ABlockOutputStream{");
        sb.append(this.writeOperationHelper.toString());
        sb.append(", blockSize=").append(this.blockSize);
        final S3ADataBlocks.DataBlock block = this.activeBlock;
        if (block != null) {
            sb.append(", activeBlock=").append(block);
        }
        sb.append('}');
        return sb.toString();
    }
    
    private void incrementWriteOperations() {
        this.fs.incrementWriteOperations();
    }
    
    private long now() {
        return System.currentTimeMillis();
    }
    
    S3AInstrumentation.OutputStreamStatistics getStatistics() {
        return this.statistics;
    }
    
    static {
        LOG = LoggerFactory.getLogger((Class)S3ABlockOutputStream.class);
    }
    
    private class MultiPartUpload
    {
        private final String uploadId;
        private final List<ListenableFuture<PartETag>> partETagsFutures;
        
        MultiPartUpload() throws IOException {
            this.uploadId = S3ABlockOutputStream.this.writeOperationHelper.initiateMultiPartUpload();
            this.partETagsFutures = new ArrayList<ListenableFuture<PartETag>>(2);
            S3ABlockOutputStream.LOG.debug("Initiated multi-part upload for {} with id '{}'", (Object)S3ABlockOutputStream.this.writeOperationHelper, (Object)this.uploadId);
        }
        
        private void uploadBlockAsync(final S3ADataBlocks.DataBlock block) throws IOException {
            S3ABlockOutputStream.LOG.debug("Queueing upload of {}", (Object)block);
            final int size = block.dataSize();
            final S3ADataBlocks.BlockUploadData uploadData = block.startUpload();
            final int currentPartNumber = this.partETagsFutures.size() + 1;
            final UploadPartRequest request = S3ABlockOutputStream.this.writeOperationHelper.newUploadPartRequest(this.uploadId, currentPartNumber, size, uploadData.getUploadStream(), uploadData.getFile());
            final long transferQueueTime = S3ABlockOutputStream.this.now();
            final BlockUploadProgress callback = new BlockUploadProgress(block, S3ABlockOutputStream.this.progressListener, transferQueueTime);
            request.setGeneralProgressListener((ProgressListener)callback);
            S3ABlockOutputStream.this.statistics.blockUploadQueued(block.dataSize());
            final ListenableFuture<PartETag> partETagFuture = (ListenableFuture<PartETag>)S3ABlockOutputStream.this.executorService.submit((Callable)new Callable<PartETag>() {
                @Override
                public PartETag call() throws Exception {
                    S3ABlockOutputStream.LOG.debug("Uploading part {} for id '{}'", (Object)currentPartNumber, (Object)MultiPartUpload.this.uploadId);
                    PartETag partETag;
                    try {
                        partETag = S3ABlockOutputStream.this.fs.uploadPart(request).getPartETag();
                        S3ABlockOutputStream.LOG.debug("Completed upload of {} to part {}", (Object)block, (Object)partETag.getETag());
                        S3ABlockOutputStream.LOG.debug("Stream statistics of {}", (Object)S3ABlockOutputStream.this.statistics);
                    }
                    finally {
                        S3AUtils.closeAll(S3ABlockOutputStream.LOG, uploadData, block);
                    }
                    return partETag;
                }
            });
            this.partETagsFutures.add(partETagFuture);
        }
        
        private List<PartETag> waitForAllPartUploads() throws IOException {
            S3ABlockOutputStream.LOG.debug("Waiting for {} uploads to complete", (Object)this.partETagsFutures.size());
            try {
                return (List<PartETag>)Futures.allAsList((Iterable)this.partETagsFutures).get();
            }
            catch (InterruptedException ie) {
                S3ABlockOutputStream.LOG.warn("Interrupted partUpload", (Throwable)ie);
                Thread.currentThread().interrupt();
                return null;
            }
            catch (ExecutionException ee) {
                S3ABlockOutputStream.LOG.debug("While waiting for upload completion", (Throwable)ee);
                S3ABlockOutputStream.LOG.debug("Cancelling futures");
                for (final ListenableFuture<PartETag> future : this.partETagsFutures) {
                    future.cancel(true);
                }
                this.abort();
                throw S3AUtils.extractException("Multi-part upload with id '" + this.uploadId + "' to " + S3ABlockOutputStream.this.key, S3ABlockOutputStream.this.key, ee);
            }
        }
        
        private CompleteMultipartUploadResult complete(final List<PartETag> partETags) throws IOException {
            int retryCount = 0;
            final String operation = String.format("Completing multi-part upload for key '%s', id '%s' with %s partitions ", S3ABlockOutputStream.this.key, this.uploadId, partETags.size());
            while (true) {
                try {
                    S3ABlockOutputStream.LOG.debug(operation);
                    return S3ABlockOutputStream.this.writeOperationHelper.completeMultipartUpload(this.uploadId, partETags);
                }
                catch (AmazonClientException e) {
                    final AmazonClientException lastException = e;
                    S3ABlockOutputStream.this.statistics.exceptionInMultipartComplete();
                    if (!this.shouldRetry(operation, lastException, retryCount++)) {
                        throw S3AUtils.translateException(operation, S3ABlockOutputStream.this.key, lastException);
                    }
                    continue;
                }
            }
        }
        
        public void abort() {
            int retryCount = 0;
            S3ABlockOutputStream.this.fs.incrementStatistic(Statistic.OBJECT_MULTIPART_UPLOAD_ABORTED);
            final String operation = String.format("Aborting multi-part upload for '%s', id '%s", S3ABlockOutputStream.this.writeOperationHelper, this.uploadId);
            while (true) {
                try {
                    S3ABlockOutputStream.LOG.debug(operation);
                    S3ABlockOutputStream.this.writeOperationHelper.abortMultipartUpload(this.uploadId);
                }
                catch (AmazonClientException e) {
                    final AmazonClientException lastException = e;
                    S3ABlockOutputStream.this.statistics.exceptionInMultipartAbort();
                    if (!this.shouldRetry(operation, lastException, retryCount++)) {
                        S3ABlockOutputStream.LOG.warn("Unable to abort multipart upload, you may need to purge  uploaded parts", (Throwable)lastException);
                        return;
                    }
                    continue;
                }
                break;
            }
        }
        
        private boolean shouldRetry(final String operation, final AmazonClientException e, final int retryCount) {
            try {
                final RetryPolicy.RetryAction retryAction = S3ABlockOutputStream.this.retryPolicy.shouldRetry((Exception)e, retryCount, 0, true);
                final boolean retry = retryAction == RetryPolicy.RetryAction.RETRY;
                if (retry) {
                    S3ABlockOutputStream.this.fs.incrementStatistic(Statistic.IGNORED_ERRORS);
                    S3ABlockOutputStream.LOG.info("Retrying {} after exception ", (Object)operation, (Object)e);
                    Thread.sleep(retryAction.delayMillis);
                }
                return retry;
            }
            catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                return false;
            }
            catch (Exception ignored) {
                return false;
            }
        }
    }
    
    private final class BlockUploadProgress implements ProgressListener
    {
        private final S3ADataBlocks.DataBlock block;
        private final ProgressListener nextListener;
        private final long transferQueueTime;
        private long transferStartTime;
        
        private BlockUploadProgress(final S3ADataBlocks.DataBlock block, final ProgressListener nextListener, final long transferQueueTime) {
            this.block = block;
            this.transferQueueTime = transferQueueTime;
            this.nextListener = nextListener;
        }
        
        public void progressChanged(final ProgressEvent progressEvent) {
            final ProgressEventType eventType = progressEvent.getEventType();
            final long bytesTransferred = progressEvent.getBytesTransferred();
            final int size = this.block.dataSize();
            switch (eventType) {
                case REQUEST_BYTE_TRANSFER_EVENT: {
                    S3ABlockOutputStream.this.statistics.bytesTransferred(bytesTransferred);
                    break;
                }
                case TRANSFER_PART_STARTED_EVENT: {
                    this.transferStartTime = S3ABlockOutputStream.this.now();
                    S3ABlockOutputStream.this.statistics.blockUploadStarted(this.transferStartTime - this.transferQueueTime, size);
                    S3ABlockOutputStream.this.incrementWriteOperations();
                    break;
                }
                case TRANSFER_PART_COMPLETED_EVENT: {
                    S3ABlockOutputStream.this.statistics.blockUploadCompleted(S3ABlockOutputStream.this.now() - this.transferStartTime, size);
                    break;
                }
                case TRANSFER_PART_FAILED_EVENT: {
                    S3ABlockOutputStream.this.statistics.blockUploadFailed(S3ABlockOutputStream.this.now() - this.transferStartTime, size);
                    S3ABlockOutputStream.LOG.warn("Transfer failure of block {}", (Object)this.block);
                    break;
                }
            }
            if (this.nextListener != null) {
                this.nextListener.progressChanged(progressEvent);
            }
        }
    }
    
    private static class ProgressableListener implements ProgressListener
    {
        private final Progressable progress;
        
        public ProgressableListener(final Progressable progress) {
            this.progress = progress;
        }
        
        public void progressChanged(final ProgressEvent progressEvent) {
            if (this.progress != null) {
                this.progress.progress();
            }
        }
    }
}
