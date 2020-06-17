// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3a;

import java.util.Collections;
import com.amazonaws.AmazonClientException;
import java.util.List;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.ObjectListing;
import java.util.HashSet;
import java.util.Iterator;
import java.util.ListIterator;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.NoSuchElementException;
import java.util.Set;
import org.apache.hadoop.fs.LocatedFileStatus;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import org.apache.hadoop.fs.RemoteIterator;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;

public class Listing
{
    private final S3AFileSystem owner;
    private static final Logger LOG;
    static final PathFilter ACCEPT_ALL;
    
    public Listing(final S3AFileSystem owner) {
        this.owner = owner;
    }
    
    ProvidedFileStatusIterator createProvidedFileStatusIterator(final FileStatus[] fileStatuses, final PathFilter filter, final FileStatusAcceptor acceptor) {
        return new ProvidedFileStatusIterator(fileStatuses, filter, acceptor);
    }
    
    FileStatusListingIterator createFileStatusListingIterator(final Path listPath, final ListObjectsRequest request, final PathFilter filter, final FileStatusAcceptor acceptor) throws IOException {
        return this.createFileStatusListingIterator(listPath, request, filter, acceptor, null);
    }
    
    FileStatusListingIterator createFileStatusListingIterator(final Path listPath, final ListObjectsRequest request, final PathFilter filter, final FileStatusAcceptor acceptor, final RemoteIterator<FileStatus> providedStatus) throws IOException {
        return new FileStatusListingIterator(new ObjectListingIterator(listPath, request), filter, acceptor, providedStatus);
    }
    
    @VisibleForTesting
    LocatedFileStatusIterator createLocatedFileStatusIterator(final RemoteIterator<FileStatus> statusIterator) {
        return new LocatedFileStatusIterator(statusIterator);
    }
    
    @VisibleForTesting
    TombstoneReconcilingIterator createTombstoneReconcilingIterator(final RemoteIterator<LocatedFileStatus> iterator, final Set<Path> tombstones) {
        return new TombstoneReconcilingIterator(iterator, tombstones);
    }
    
    static {
        LOG = S3AFileSystem.LOG;
        ACCEPT_ALL = (PathFilter)new PathFilter() {
            public boolean accept(final Path file) {
                return true;
            }
            
            @Override
            public String toString() {
                return "ACCEPT_ALL";
            }
        };
    }
    
    static final class SingleStatusRemoteIterator implements RemoteIterator<LocatedFileStatus>
    {
        private LocatedFileStatus status;
        
        public SingleStatusRemoteIterator(final LocatedFileStatus status) {
            this.status = status;
        }
        
        public boolean hasNext() throws IOException {
            return this.status != null;
        }
        
        public LocatedFileStatus next() throws IOException {
            if (this.hasNext()) {
                final LocatedFileStatus s = this.status;
                this.status = null;
                return s;
            }
            throw new NoSuchElementException();
        }
    }
    
    static class ProvidedFileStatusIterator implements RemoteIterator<FileStatus>
    {
        private final ArrayList<FileStatus> filteredStatusList;
        private int index;
        
        ProvidedFileStatusIterator(final FileStatus[] fileStatuses, final PathFilter filter, final FileStatusAcceptor acceptor) {
            this.index = 0;
            Preconditions.checkArgument(fileStatuses != null, (Object)"Null status list!");
            this.filteredStatusList = new ArrayList<FileStatus>(fileStatuses.length);
            for (final FileStatus status : fileStatuses) {
                if (filter.accept(status.getPath()) && acceptor.accept(status)) {
                    this.filteredStatusList.add(status);
                }
            }
            this.filteredStatusList.trimToSize();
        }
        
        public boolean hasNext() throws IOException {
            return this.index < this.filteredStatusList.size();
        }
        
        public FileStatus next() throws IOException {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }
            return this.filteredStatusList.get(this.index++);
        }
    }
    
    class FileStatusListingIterator implements RemoteIterator<FileStatus>
    {
        private final ObjectListingIterator source;
        private final PathFilter filter;
        private final FileStatusAcceptor acceptor;
        private int batchSize;
        private ListIterator<FileStatus> statusBatchIterator;
        private final Set<FileStatus> providedStatus;
        private Iterator<FileStatus> providedStatusIterator;
        
        FileStatusListingIterator(final ObjectListingIterator source, final PathFilter filter, final FileStatusAcceptor acceptor, final RemoteIterator<FileStatus> providedStatus) throws IOException {
            this.source = source;
            this.filter = filter;
            this.acceptor = acceptor;
            this.providedStatus = new HashSet<FileStatus>();
            while (providedStatus != null && providedStatus.hasNext()) {
                final FileStatus status = (FileStatus)providedStatus.next();
                if (filter.accept(status.getPath()) && acceptor.accept(status)) {
                    this.providedStatus.add(status);
                }
            }
            this.requestNextBatch();
        }
        
        public boolean hasNext() throws IOException {
            return this.sourceHasNext() || this.providedStatusIterator.hasNext();
        }
        
        private boolean sourceHasNext() throws IOException {
            if (this.statusBatchIterator.hasNext() || this.requestNextBatch()) {
                return true;
            }
            if (this.providedStatusIterator == null) {
                Listing.LOG.debug("Start iterating the provided status.");
                this.providedStatusIterator = this.providedStatus.iterator();
            }
            return false;
        }
        
        public FileStatus next() throws IOException {
            FileStatus status;
            if (this.sourceHasNext()) {
                status = this.statusBatchIterator.next();
                Listing.LOG.debug("Removing the status from provided file status {}", (Object)status);
                this.providedStatus.remove(status);
            }
            else {
                if (!this.providedStatusIterator.hasNext()) {
                    throw new NoSuchElementException();
                }
                status = this.providedStatusIterator.next();
                Listing.LOG.debug("Returning provided file status {}", (Object)status);
            }
            return status;
        }
        
        private boolean requestNextBatch() throws IOException {
            while (this.source.hasNext()) {
                if (this.buildNextStatusBatch(this.source.next())) {
                    return true;
                }
                Listing.LOG.debug("All entries in batch were filtered...continuing");
            }
            return false;
        }
        
        private boolean buildNextStatusBatch(final ObjectListing objects) {
            int added = 0;
            int ignored = 0;
            final List<FileStatus> stats = new ArrayList<FileStatus>(objects.getObjectSummaries().size() + objects.getCommonPrefixes().size());
            for (final S3ObjectSummary summary : objects.getObjectSummaries()) {
                final String key = summary.getKey();
                final Path keyPath = Listing.this.owner.keyToQualifiedPath(key);
                if (Listing.LOG.isDebugEnabled()) {
                    Listing.LOG.debug("{}: {}", (Object)keyPath, (Object)S3AUtils.stringify(summary));
                }
                if (this.acceptor.accept(keyPath, summary) && this.filter.accept(keyPath)) {
                    final FileStatus status = S3AUtils.createFileStatus(keyPath, summary, Listing.this.owner.getDefaultBlockSize(keyPath), Listing.this.owner.getUsername());
                    Listing.LOG.debug("Adding: {}", (Object)status);
                    stats.add(status);
                    ++added;
                }
                else {
                    Listing.LOG.debug("Ignoring: {}", (Object)keyPath);
                    ++ignored;
                }
            }
            for (final String prefix : objects.getCommonPrefixes()) {
                final Path keyPath2 = Listing.this.owner.keyToQualifiedPath(prefix);
                if (this.acceptor.accept(keyPath2, prefix) && this.filter.accept(keyPath2)) {
                    final FileStatus status2 = new S3AFileStatus(Tristate.FALSE, keyPath2, Listing.this.owner.getUsername());
                    Listing.LOG.debug("Adding directory: {}", (Object)status2);
                    ++added;
                    stats.add(status2);
                }
                else {
                    Listing.LOG.debug("Ignoring directory: {}", (Object)keyPath2);
                    ++ignored;
                }
            }
            this.batchSize = stats.size();
            this.statusBatchIterator = stats.listIterator();
            final boolean hasNext = this.statusBatchIterator.hasNext();
            Listing.LOG.debug("Added {} entries; ignored {}; hasNext={}; hasMoreObjects={}", new Object[] { added, ignored, hasNext, objects.isTruncated() });
            return hasNext;
        }
        
        public int getBatchSize() {
            return this.batchSize;
        }
    }
    
    class ObjectListingIterator implements RemoteIterator<ObjectListing>
    {
        private final Path listPath;
        private ObjectListing objects;
        private boolean firstListing;
        private int listingCount;
        private int maxKeys;
        
        ObjectListingIterator(final Path listPath, final ListObjectsRequest request) {
            this.firstListing = true;
            this.listingCount = 1;
            this.listPath = listPath;
            this.maxKeys = Listing.this.owner.getMaxKeys();
            this.objects = Listing.this.owner.listObjects(request);
        }
        
        public boolean hasNext() throws IOException {
            return this.firstListing || this.objects.isTruncated();
        }
        
        public ObjectListing next() throws IOException {
            if (this.firstListing) {
                this.firstListing = false;
            }
            else {
                try {
                    if (!this.objects.isTruncated()) {
                        throw new NoSuchElementException("No more results in listing of " + this.listPath);
                    }
                    Listing.LOG.debug("[{}], Requesting next {} objects under {}", new Object[] { this.listingCount, this.maxKeys, this.listPath });
                    this.objects = Listing.this.owner.continueListObjects(this.objects);
                    ++this.listingCount;
                    Listing.LOG.debug("New listing status: {}", (Object)this);
                }
                catch (AmazonClientException e) {
                    throw S3AUtils.translateException("listObjects()", this.listPath, e);
                }
            }
            return this.objects;
        }
        
        @Override
        public String toString() {
            return "Object listing iterator against " + this.listPath + "; listing count " + this.listingCount + "; isTruncated=" + this.objects.isTruncated();
        }
        
        public Path getListPath() {
            return this.listPath;
        }
        
        public int getListingCount() {
            return this.listingCount;
        }
    }
    
    static class AcceptFilesOnly implements FileStatusAcceptor
    {
        private final Path qualifiedPath;
        
        public AcceptFilesOnly(final Path qualifiedPath) {
            this.qualifiedPath = qualifiedPath;
        }
        
        @Override
        public boolean accept(final Path keyPath, final S3ObjectSummary summary) {
            return !keyPath.equals((Object)this.qualifiedPath) && !summary.getKey().endsWith("_$folder$") && !S3AUtils.objectRepresentsDirectory(summary.getKey(), summary.getSize());
        }
        
        @Override
        public boolean accept(final Path keyPath, final String prefix) {
            return false;
        }
        
        @Override
        public boolean accept(final FileStatus status) {
            return status != null && status.isFile();
        }
    }
    
    class LocatedFileStatusIterator implements RemoteIterator<LocatedFileStatus>
    {
        private final RemoteIterator<FileStatus> statusIterator;
        
        LocatedFileStatusIterator(final RemoteIterator<FileStatus> statusIterator) {
            this.statusIterator = statusIterator;
        }
        
        public boolean hasNext() throws IOException {
            return this.statusIterator.hasNext();
        }
        
        public LocatedFileStatus next() throws IOException {
            return Listing.this.owner.toLocatedFileStatus((FileStatus)this.statusIterator.next());
        }
    }
    
    static class TombstoneReconcilingIterator implements RemoteIterator<LocatedFileStatus>
    {
        private LocatedFileStatus next;
        private final RemoteIterator<LocatedFileStatus> iterator;
        private final Set<Path> tombstones;
        
        TombstoneReconcilingIterator(final RemoteIterator<LocatedFileStatus> iterator, final Set<Path> tombstones) {
            this.next = null;
            this.iterator = iterator;
            if (tombstones != null) {
                this.tombstones = tombstones;
            }
            else {
                this.tombstones = (Set<Path>)Collections.EMPTY_SET;
            }
        }
        
        private boolean fetch() throws IOException {
            while (this.next == null && this.iterator.hasNext()) {
                final LocatedFileStatus candidate = (LocatedFileStatus)this.iterator.next();
                if (!this.tombstones.contains(candidate.getPath())) {
                    this.next = candidate;
                    return true;
                }
            }
            return false;
        }
        
        public boolean hasNext() throws IOException {
            return this.next != null || this.fetch();
        }
        
        public LocatedFileStatus next() throws IOException {
            if (this.hasNext()) {
                final LocatedFileStatus result = this.next;
                this.next = null;
                this.fetch();
                return result;
            }
            throw new NoSuchElementException();
        }
    }
    
    static class AcceptAllButS3nDirs implements FileStatusAcceptor
    {
        @Override
        public boolean accept(final Path keyPath, final S3ObjectSummary summary) {
            return !summary.getKey().endsWith("_$folder$");
        }
        
        @Override
        public boolean accept(final Path keyPath, final String prefix) {
            return !keyPath.toString().endsWith("_$folder$");
        }
        
        @Override
        public boolean accept(final FileStatus status) {
            return !status.getPath().toString().endsWith("_$folder$");
        }
    }
    
    static class AcceptAllButSelfAndS3nDirs implements FileStatusAcceptor
    {
        private final Path qualifiedPath;
        
        public AcceptAllButSelfAndS3nDirs(final Path qualifiedPath) {
            this.qualifiedPath = qualifiedPath;
        }
        
        @Override
        public boolean accept(final Path keyPath, final S3ObjectSummary summary) {
            return !keyPath.equals((Object)this.qualifiedPath) && !summary.getKey().endsWith("_$folder$");
        }
        
        @Override
        public boolean accept(final Path keyPath, final String prefix) {
            return !keyPath.equals((Object)this.qualifiedPath);
        }
        
        @Override
        public boolean accept(final FileStatus status) {
            return status != null && !status.getPath().equals((Object)this.qualifiedPath);
        }
    }
    
    interface FileStatusAcceptor
    {
        boolean accept(final Path p0, final S3ObjectSummary p1);
        
        boolean accept(final Path p0, final String p1);
        
        boolean accept(final FileStatus p0);
    }
}
