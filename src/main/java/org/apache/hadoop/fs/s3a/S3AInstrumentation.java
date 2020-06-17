// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3a;

import java.util.concurrent.atomic.AtomicInteger;
import java.io.Closeable;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FileSystem;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.metrics2.lib.MutableMetric;
import java.util.Iterator;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricStringBuilder;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.metrics2.lib.Interns;
import java.util.UUID;
import java.util.HashMap;
import java.net.URI;
import java.util.Map;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.slf4j.Logger;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.annotation.Metrics;

@Metrics(about = "Metrics for S3a", context = "S3AFileSystem")
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class S3AInstrumentation
{
    private static final Logger LOG;
    public static final String CONTEXT = "S3AFileSystem";
    private final MetricsRegistry registry;
    private final MutableCounterLong streamOpenOperations;
    private final MutableCounterLong streamCloseOperations;
    private final MutableCounterLong streamClosed;
    private final MutableCounterLong streamAborted;
    private final MutableCounterLong streamSeekOperations;
    private final MutableCounterLong streamReadExceptions;
    private final MutableCounterLong streamForwardSeekOperations;
    private final MutableCounterLong streamBackwardSeekOperations;
    private final MutableCounterLong streamBytesSkippedOnSeek;
    private final MutableCounterLong streamBytesBackwardsOnSeek;
    private final MutableCounterLong streamBytesRead;
    private final MutableCounterLong streamReadOperations;
    private final MutableCounterLong streamReadFullyOperations;
    private final MutableCounterLong streamReadsIncomplete;
    private final MutableCounterLong streamBytesReadInClose;
    private final MutableCounterLong streamBytesDiscardedInAbort;
    private final MutableCounterLong ignoredErrors;
    private final MutableCounterLong numberOfFilesCreated;
    private final MutableCounterLong numberOfFilesCopied;
    private final MutableCounterLong bytesOfFilesCopied;
    private final MutableCounterLong numberOfFilesDeleted;
    private final MutableCounterLong numberOfFakeDirectoryDeletes;
    private final MutableCounterLong numberOfDirectoriesCreated;
    private final MutableCounterLong numberOfDirectoriesDeleted;
    private final Map<String, MutableCounterLong> streamMetrics;
    private final S3GuardInstrumentation s3GuardInstrumentation;
    private static final Statistic[] COUNTERS_TO_CREATE;
    private static final Statistic[] GAUGES_TO_CREATE;
    
    public S3AInstrumentation(final URI name) {
        this.registry = new MetricsRegistry("S3AFileSystem").setContext("S3AFileSystem");
        this.streamMetrics = new HashMap<String, MutableCounterLong>(30);
        this.s3GuardInstrumentation = new S3GuardInstrumentation();
        final UUID fileSystemInstanceId = UUID.randomUUID();
        this.registry.tag("FileSystemId", "A unique identifier for the FS ", fileSystemInstanceId.toString() + "-" + name.getHost());
        this.registry.tag("fsURI", "URI of this filesystem", name.toString());
        this.streamOpenOperations = this.streamCounter(Statistic.STREAM_OPENED);
        this.streamCloseOperations = this.streamCounter(Statistic.STREAM_CLOSE_OPERATIONS);
        this.streamClosed = this.streamCounter(Statistic.STREAM_CLOSED);
        this.streamAborted = this.streamCounter(Statistic.STREAM_ABORTED);
        this.streamSeekOperations = this.streamCounter(Statistic.STREAM_SEEK_OPERATIONS);
        this.streamReadExceptions = this.streamCounter(Statistic.STREAM_READ_EXCEPTIONS);
        this.streamForwardSeekOperations = this.streamCounter(Statistic.STREAM_FORWARD_SEEK_OPERATIONS);
        this.streamBackwardSeekOperations = this.streamCounter(Statistic.STREAM_BACKWARD_SEEK_OPERATIONS);
        this.streamBytesSkippedOnSeek = this.streamCounter(Statistic.STREAM_SEEK_BYTES_SKIPPED);
        this.streamBytesBackwardsOnSeek = this.streamCounter(Statistic.STREAM_SEEK_BYTES_BACKWARDS);
        this.streamBytesRead = this.streamCounter(Statistic.STREAM_SEEK_BYTES_READ);
        this.streamReadOperations = this.streamCounter(Statistic.STREAM_READ_OPERATIONS);
        this.streamReadFullyOperations = this.streamCounter(Statistic.STREAM_READ_FULLY_OPERATIONS);
        this.streamReadsIncomplete = this.streamCounter(Statistic.STREAM_READ_OPERATIONS_INCOMPLETE);
        this.streamBytesReadInClose = this.streamCounter(Statistic.STREAM_CLOSE_BYTES_READ);
        this.streamBytesDiscardedInAbort = this.streamCounter(Statistic.STREAM_ABORT_BYTES_DISCARDED);
        this.numberOfFilesCreated = this.counter(Statistic.FILES_CREATED);
        this.numberOfFilesCopied = this.counter(Statistic.FILES_COPIED);
        this.bytesOfFilesCopied = this.counter(Statistic.FILES_COPIED_BYTES);
        this.numberOfFilesDeleted = this.counter(Statistic.FILES_DELETED);
        this.numberOfFakeDirectoryDeletes = this.counter(Statistic.FAKE_DIRECTORIES_DELETED);
        this.numberOfDirectoriesCreated = this.counter(Statistic.DIRECTORIES_CREATED);
        this.numberOfDirectoriesDeleted = this.counter(Statistic.DIRECTORIES_DELETED);
        this.ignoredErrors = this.counter(Statistic.IGNORED_ERRORS);
        for (final Statistic statistic : S3AInstrumentation.COUNTERS_TO_CREATE) {
            this.counter(statistic);
        }
        for (final Statistic statistic : S3AInstrumentation.GAUGES_TO_CREATE) {
            this.gauge(statistic.getSymbol(), statistic.getDescription());
        }
        this.quantiles(Statistic.S3GUARD_METADATASTORE_PUT_PATH_LATENCY, "ops", "latency", 1);
    }
    
    protected final MutableCounterLong counter(final String name, final String desc) {
        return this.registry.newCounter(name, desc, 0L);
    }
    
    protected final MutableCounterLong streamCounter(final String name, final String desc) {
        final MutableCounterLong counter = new MutableCounterLong(Interns.info(name, desc), 0L);
        this.streamMetrics.put(name, counter);
        return counter;
    }
    
    protected final MutableCounterLong counter(final Statistic op) {
        return this.counter(op.getSymbol(), op.getDescription());
    }
    
    protected final MutableCounterLong streamCounter(final Statistic op) {
        return this.streamCounter(op.getSymbol(), op.getDescription());
    }
    
    protected final MutableGaugeLong gauge(final String name, final String desc) {
        return this.registry.newGauge(name, desc, 0L);
    }
    
    protected final MutableQuantiles quantiles(final Statistic op, final String sampleName, final String valueName, final int interval) {
        return this.registry.newQuantiles(op.getSymbol(), op.getDescription(), sampleName, valueName, interval);
    }
    
    public MetricsRegistry getRegistry() {
        return this.registry;
    }
    
    public String dump(final String prefix, final String separator, final String suffix, final boolean all) {
        final MetricStringBuilder metricBuilder = new MetricStringBuilder((MetricsCollector)null, prefix, separator, suffix);
        this.registry.snapshot((MetricsRecordBuilder)metricBuilder, all);
        for (final Map.Entry<String, MutableCounterLong> entry : this.streamMetrics.entrySet()) {
            metricBuilder.tuple((String)entry.getKey(), Long.toString(entry.getValue().value()));
        }
        return metricBuilder.toString();
    }
    
    public long getCounterValue(final Statistic statistic) {
        return this.getCounterValue(statistic.getSymbol());
    }
    
    public long getCounterValue(final String name) {
        final MutableCounterLong counter = this.lookupCounter(name);
        return (counter == null) ? 0L : counter.value();
    }
    
    private MutableCounterLong lookupCounter(final String name) {
        final MutableMetric metric = this.lookupMetric(name);
        if (metric == null) {
            return null;
        }
        if (!(metric instanceof MutableCounterLong)) {
            throw new IllegalStateException("Metric " + name + " is not a MutableCounterLong: " + metric);
        }
        return (MutableCounterLong)metric;
    }
    
    public MutableGaugeLong lookupGauge(final String name) {
        final MutableMetric metric = this.lookupMetric(name);
        if (metric == null) {
            S3AInstrumentation.LOG.debug("No gauge {}", (Object)name);
        }
        return (MutableGaugeLong)metric;
    }
    
    public MutableQuantiles lookupQuantiles(final String name) {
        final MutableMetric metric = this.lookupMetric(name);
        if (metric == null) {
            S3AInstrumentation.LOG.debug("No quantiles {}", (Object)name);
        }
        return (MutableQuantiles)metric;
    }
    
    public MutableMetric lookupMetric(final String name) {
        MutableMetric metric = this.getRegistry().get(name);
        if (metric == null) {
            metric = (MutableMetric)this.streamMetrics.get(name);
        }
        return metric;
    }
    
    public void fileCreated() {
        this.numberOfFilesCreated.incr();
    }
    
    public void fileDeleted(final int count) {
        this.numberOfFilesDeleted.incr((long)count);
    }
    
    public void fakeDirsDeleted(final int count) {
        this.numberOfFakeDirectoryDeletes.incr((long)count);
    }
    
    public void directoryCreated() {
        this.numberOfDirectoriesCreated.incr();
    }
    
    public void directoryDeleted() {
        this.numberOfDirectoriesDeleted.incr();
    }
    
    public void filesCopied(final int files, final long size) {
        this.numberOfFilesCopied.incr((long)files);
        this.bytesOfFilesCopied.incr(size);
    }
    
    public void errorIgnored() {
        this.ignoredErrors.incr();
    }
    
    public void incrementCounter(final Statistic op, final long count) {
        final MutableCounterLong counter = this.lookupCounter(op.getSymbol());
        if (counter != null) {
            counter.incr(count);
        }
    }
    
    public void addValueToQuantiles(final Statistic op, final long value) {
        final MutableQuantiles quantiles = this.lookupQuantiles(op.getSymbol());
        if (quantiles != null) {
            quantiles.add(value);
        }
    }
    
    public void incrementCounter(final Statistic op, final AtomicLong count) {
        this.incrementCounter(op, count.get());
    }
    
    public void incrementGauge(final Statistic op, final long count) {
        final MutableGaugeLong gauge = this.lookupGauge(op.getSymbol());
        if (gauge != null) {
            gauge.incr(count);
        }
        else {
            S3AInstrumentation.LOG.debug("No Gauge: " + op);
        }
    }
    
    public void decrementGauge(final Statistic op, final long count) {
        final MutableGaugeLong gauge = this.lookupGauge(op.getSymbol());
        if (gauge != null) {
            gauge.decr(count);
        }
        else {
            S3AInstrumentation.LOG.debug("No Gauge: {}", (Object)op);
        }
    }
    
    InputStreamStatistics newInputStreamStatistics() {
        return new InputStreamStatistics();
    }
    
    public S3GuardInstrumentation getS3GuardInstrumentation() {
        return this.s3GuardInstrumentation;
    }
    
    private void mergeInputStreamStatistics(final InputStreamStatistics statistics) {
        this.streamOpenOperations.incr(statistics.openOperations);
        this.streamCloseOperations.incr(statistics.closeOperations);
        this.streamClosed.incr(statistics.closed);
        this.streamAborted.incr(statistics.aborted);
        this.streamSeekOperations.incr(statistics.seekOperations);
        this.streamReadExceptions.incr(statistics.readExceptions);
        this.streamForwardSeekOperations.incr(statistics.forwardSeekOperations);
        this.streamBytesSkippedOnSeek.incr(statistics.bytesSkippedOnSeek);
        this.streamBackwardSeekOperations.incr(statistics.backwardSeekOperations);
        this.streamBytesBackwardsOnSeek.incr(statistics.bytesBackwardsOnSeek);
        this.streamBytesRead.incr(statistics.bytesRead);
        this.streamReadOperations.incr(statistics.readOperations);
        this.streamReadFullyOperations.incr(statistics.readFullyOperations);
        this.streamReadsIncomplete.incr(statistics.readsIncomplete);
        this.streamBytesReadInClose.incr(statistics.bytesReadInClose);
        this.streamBytesDiscardedInAbort.incr(statistics.bytesDiscardedInAbort);
    }
    
    OutputStreamStatistics newOutputStreamStatistics(final FileSystem.Statistics statistics) {
        return new OutputStreamStatistics(statistics);
    }
    
    private void mergeOutputStreamStatistics(final OutputStreamStatistics statistics) {
        this.incrementCounter(Statistic.STREAM_WRITE_TOTAL_TIME, statistics.totalUploadDuration());
        this.incrementCounter(Statistic.STREAM_WRITE_QUEUE_DURATION, statistics.queueDuration);
        this.incrementCounter(Statistic.STREAM_WRITE_TOTAL_DATA, statistics.bytesUploaded);
        this.incrementCounter(Statistic.STREAM_WRITE_BLOCK_UPLOADS, statistics.blockUploadsCompleted);
    }
    
    static {
        LOG = LoggerFactory.getLogger((Class)S3AInstrumentation.class);
        COUNTERS_TO_CREATE = new Statistic[] { Statistic.INVOCATION_COPY_FROM_LOCAL_FILE, Statistic.INVOCATION_EXISTS, Statistic.INVOCATION_GET_FILE_STATUS, Statistic.INVOCATION_GLOB_STATUS, Statistic.INVOCATION_IS_DIRECTORY, Statistic.INVOCATION_IS_FILE, Statistic.INVOCATION_LIST_FILES, Statistic.INVOCATION_LIST_LOCATED_STATUS, Statistic.INVOCATION_LIST_STATUS, Statistic.INVOCATION_MKDIRS, Statistic.INVOCATION_RENAME, Statistic.OBJECT_COPY_REQUESTS, Statistic.OBJECT_DELETE_REQUESTS, Statistic.OBJECT_LIST_REQUESTS, Statistic.OBJECT_CONTINUE_LIST_REQUESTS, Statistic.OBJECT_METADATA_REQUESTS, Statistic.OBJECT_MULTIPART_UPLOAD_ABORTED, Statistic.OBJECT_PUT_BYTES, Statistic.OBJECT_PUT_REQUESTS, Statistic.OBJECT_PUT_REQUESTS_COMPLETED, Statistic.STREAM_WRITE_FAILURES, Statistic.STREAM_WRITE_BLOCK_UPLOADS, Statistic.STREAM_WRITE_BLOCK_UPLOADS_COMMITTED, Statistic.STREAM_WRITE_BLOCK_UPLOADS_ABORTED, Statistic.STREAM_WRITE_TOTAL_TIME, Statistic.STREAM_WRITE_TOTAL_DATA, Statistic.S3GUARD_METADATASTORE_PUT_PATH_REQUEST, Statistic.S3GUARD_METADATASTORE_INITIALIZATION };
        GAUGES_TO_CREATE = new Statistic[] { Statistic.OBJECT_PUT_REQUESTS_ACTIVE, Statistic.OBJECT_PUT_BYTES_PENDING, Statistic.STREAM_WRITE_BLOCK_UPLOADS_ACTIVE, Statistic.STREAM_WRITE_BLOCK_UPLOADS_PENDING, Statistic.STREAM_WRITE_BLOCK_UPLOADS_DATA_PENDING };
    }
    
    @InterfaceAudience.Private
    @InterfaceStability.Unstable
    public final class InputStreamStatistics implements AutoCloseable
    {
        public long openOperations;
        public long closeOperations;
        public long closed;
        public long aborted;
        public long seekOperations;
        public long readExceptions;
        public long forwardSeekOperations;
        public long backwardSeekOperations;
        public long bytesRead;
        public long bytesSkippedOnSeek;
        public long bytesBackwardsOnSeek;
        public long readOperations;
        public long readFullyOperations;
        public long readsIncomplete;
        public long bytesReadInClose;
        public long bytesDiscardedInAbort;
        
        private InputStreamStatistics() {
        }
        
        public void seekBackwards(final long negativeOffset) {
            ++this.seekOperations;
            ++this.backwardSeekOperations;
            this.bytesBackwardsOnSeek -= negativeOffset;
        }
        
        public void seekForwards(final long skipped) {
            ++this.seekOperations;
            ++this.forwardSeekOperations;
            if (skipped > 0L) {
                this.bytesSkippedOnSeek += skipped;
            }
        }
        
        public void streamOpened() {
            ++this.openOperations;
        }
        
        public void streamClose(final boolean abortedConnection, final long remainingInCurrentRequest) {
            ++this.closeOperations;
            if (abortedConnection) {
                ++this.aborted;
                this.bytesDiscardedInAbort += remainingInCurrentRequest;
            }
            else {
                ++this.closed;
                this.bytesReadInClose += remainingInCurrentRequest;
            }
        }
        
        public void readException() {
            ++this.readExceptions;
        }
        
        public void bytesRead(final long bytes) {
            if (bytes > 0L) {
                this.bytesRead += bytes;
            }
        }
        
        public void readOperationStarted(final long pos, final long len) {
            ++this.readOperations;
        }
        
        public void readFullyOperationStarted(final long pos, final long len) {
            ++this.readFullyOperations;
        }
        
        public void readOperationCompleted(final int requested, final int actual) {
            if (requested > actual) {
                ++this.readsIncomplete;
            }
        }
        
        @Override
        public void close() {
            S3AInstrumentation.this.mergeInputStreamStatistics(this);
        }
        
        @InterfaceStability.Unstable
        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("StreamStatistics{");
            sb.append("OpenOperations=").append(this.openOperations);
            sb.append(", CloseOperations=").append(this.closeOperations);
            sb.append(", Closed=").append(this.closed);
            sb.append(", Aborted=").append(this.aborted);
            sb.append(", SeekOperations=").append(this.seekOperations);
            sb.append(", ReadExceptions=").append(this.readExceptions);
            sb.append(", ForwardSeekOperations=").append(this.forwardSeekOperations);
            sb.append(", BackwardSeekOperations=").append(this.backwardSeekOperations);
            sb.append(", BytesSkippedOnSeek=").append(this.bytesSkippedOnSeek);
            sb.append(", BytesBackwardsOnSeek=").append(this.bytesBackwardsOnSeek);
            sb.append(", BytesRead=").append(this.bytesRead);
            sb.append(", BytesRead excluding skipped=").append(this.bytesRead - this.bytesSkippedOnSeek);
            sb.append(", ReadOperations=").append(this.readOperations);
            sb.append(", ReadFullyOperations=").append(this.readFullyOperations);
            sb.append(", ReadsIncomplete=").append(this.readsIncomplete);
            sb.append(", BytesReadInClose=").append(this.bytesReadInClose);
            sb.append(", BytesDiscardedInAbort=").append(this.bytesDiscardedInAbort);
            sb.append('}');
            return sb.toString();
        }
    }
    
    @InterfaceAudience.Private
    @InterfaceStability.Unstable
    public final class OutputStreamStatistics implements Closeable
    {
        private final AtomicLong blocksSubmitted;
        private final AtomicLong blocksInQueue;
        private final AtomicLong blocksActive;
        private final AtomicLong blockUploadsCompleted;
        private final AtomicLong blockUploadsFailed;
        private final AtomicLong bytesPendingUpload;
        private final AtomicLong bytesUploaded;
        private final AtomicLong transferDuration;
        private final AtomicLong queueDuration;
        private final AtomicLong exceptionsInMultipartFinalize;
        private final AtomicInteger blocksAllocated;
        private final AtomicInteger blocksReleased;
        private FileSystem.Statistics statistics;
        
        public OutputStreamStatistics(final FileSystem.Statistics statistics) {
            this.blocksSubmitted = new AtomicLong(0L);
            this.blocksInQueue = new AtomicLong(0L);
            this.blocksActive = new AtomicLong(0L);
            this.blockUploadsCompleted = new AtomicLong(0L);
            this.blockUploadsFailed = new AtomicLong(0L);
            this.bytesPendingUpload = new AtomicLong(0L);
            this.bytesUploaded = new AtomicLong(0L);
            this.transferDuration = new AtomicLong(0L);
            this.queueDuration = new AtomicLong(0L);
            this.exceptionsInMultipartFinalize = new AtomicLong(0L);
            this.blocksAllocated = new AtomicInteger(0);
            this.blocksReleased = new AtomicInteger(0);
            this.statistics = statistics;
        }
        
        void blockAllocated() {
            this.blocksAllocated.incrementAndGet();
        }
        
        void blockReleased() {
            this.blocksReleased.incrementAndGet();
        }
        
        void blockUploadQueued(final int blockSize) {
            this.blocksSubmitted.incrementAndGet();
            this.blocksInQueue.incrementAndGet();
            this.bytesPendingUpload.addAndGet(blockSize);
            S3AInstrumentation.this.incrementGauge(Statistic.STREAM_WRITE_BLOCK_UPLOADS_PENDING, 1L);
            S3AInstrumentation.this.incrementGauge(Statistic.STREAM_WRITE_BLOCK_UPLOADS_DATA_PENDING, blockSize);
        }
        
        void blockUploadStarted(final long duration, final int blockSize) {
            this.queueDuration.addAndGet(duration);
            this.blocksInQueue.decrementAndGet();
            this.blocksActive.incrementAndGet();
            S3AInstrumentation.this.incrementGauge(Statistic.STREAM_WRITE_BLOCK_UPLOADS_PENDING, -1L);
            S3AInstrumentation.this.incrementGauge(Statistic.STREAM_WRITE_BLOCK_UPLOADS_ACTIVE, 1L);
        }
        
        void blockUploadCompleted(final long duration, final int blockSize) {
            this.transferDuration.addAndGet(duration);
            S3AInstrumentation.this.incrementGauge(Statistic.STREAM_WRITE_BLOCK_UPLOADS_ACTIVE, -1L);
            this.blocksActive.decrementAndGet();
            this.blockUploadsCompleted.incrementAndGet();
        }
        
        void blockUploadFailed(final long duration, final int blockSize) {
            this.blockUploadsFailed.incrementAndGet();
        }
        
        void bytesTransferred(final long byteCount) {
            this.bytesUploaded.addAndGet(byteCount);
            this.statistics.incrementBytesWritten(byteCount);
            this.bytesPendingUpload.addAndGet(-byteCount);
            S3AInstrumentation.this.incrementGauge(Statistic.STREAM_WRITE_BLOCK_UPLOADS_DATA_PENDING, -byteCount);
        }
        
        void exceptionInMultipartComplete() {
            this.exceptionsInMultipartFinalize.incrementAndGet();
        }
        
        void exceptionInMultipartAbort() {
            this.exceptionsInMultipartFinalize.incrementAndGet();
        }
        
        public long getBytesPendingUpload() {
            return this.bytesPendingUpload.get();
        }
        
        @Override
        public void close() {
            if (this.bytesPendingUpload.get() > 0L) {
                S3AInstrumentation.LOG.warn("Closing output stream statistics while data is still marked as pending upload in {}", (Object)this);
            }
            S3AInstrumentation.this.mergeOutputStreamStatistics(this);
        }
        
        long averageQueueTime() {
            return (this.blocksSubmitted.get() > 0L) ? (this.queueDuration.get() / this.blocksSubmitted.get()) : 0L;
        }
        
        double effectiveBandwidth() {
            final double duration = this.totalUploadDuration() / 1000.0;
            return (duration > 0.0) ? (this.bytesUploaded.get() / duration) : 0.0;
        }
        
        long totalUploadDuration() {
            return this.queueDuration.get() + this.transferDuration.get();
        }
        
        public int blocksAllocated() {
            return this.blocksAllocated.get();
        }
        
        public int blocksReleased() {
            return this.blocksReleased.get();
        }
        
        public int blocksActivelyAllocated() {
            return this.blocksAllocated.get() - this.blocksReleased.get();
        }
        
        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("OutputStreamStatistics{");
            sb.append("blocksSubmitted=").append(this.blocksSubmitted);
            sb.append(", blocksInQueue=").append(this.blocksInQueue);
            sb.append(", blocksActive=").append(this.blocksActive);
            sb.append(", blockUploadsCompleted=").append(this.blockUploadsCompleted);
            sb.append(", blockUploadsFailed=").append(this.blockUploadsFailed);
            sb.append(", bytesPendingUpload=").append(this.bytesPendingUpload);
            sb.append(", bytesUploaded=").append(this.bytesUploaded);
            sb.append(", blocksAllocated=").append(this.blocksAllocated);
            sb.append(", blocksReleased=").append(this.blocksReleased);
            sb.append(", blocksActivelyAllocated=").append(this.blocksActivelyAllocated());
            sb.append(", exceptionsInMultipartFinalize=").append(this.exceptionsInMultipartFinalize);
            sb.append(", transferDuration=").append(this.transferDuration).append(" ms");
            sb.append(", queueDuration=").append(this.queueDuration).append(" ms");
            sb.append(", averageQueueTime=").append(this.averageQueueTime()).append(" ms");
            sb.append(", totalUploadDuration=").append(this.totalUploadDuration()).append(" ms");
            sb.append(", effectiveBandwidth=").append(this.effectiveBandwidth()).append(" bytes/s");
            sb.append('}');
            return sb.toString();
        }
    }
    
    public final class S3GuardInstrumentation
    {
        public void initialized() {
            S3AInstrumentation.this.incrementCounter(Statistic.S3GUARD_METADATASTORE_INITIALIZATION, 1L);
        }
        
        public void storeClosed() {
        }
    }
}
