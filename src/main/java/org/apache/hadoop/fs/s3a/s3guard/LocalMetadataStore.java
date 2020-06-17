// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3a.s3guard;

import org.slf4j.LoggerFactory;
import com.google.common.annotations.VisibleForTesting;
import java.util.LinkedList;
import org.apache.hadoop.fs.FileStatus;
import java.util.Iterator;
import java.util.Collection;
import org.apache.hadoop.fs.s3a.Tristate;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import java.io.IOException;
import java.net.URI;
import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;

public class LocalMetadataStore implements MetadataStore
{
    public static final Logger LOG;
    public static final int DEFAULT_MAX_RECORDS = 128;
    public static final String CONF_MAX_RECORDS = "fs.metadatastore.local.max_records";
    private LruHashMap<Path, PathMetadata> fileHash;
    private LruHashMap<Path, DirListingMetadata> dirHash;
    private FileSystem fs;
    private String uriHost;
    
    @Override
    public void initialize(final FileSystem fileSystem) throws IOException {
        Preconditions.checkNotNull((Object)fileSystem);
        this.fs = fileSystem;
        final URI fsURI = this.fs.getUri();
        this.uriHost = fsURI.getHost();
        if (this.uriHost != null && this.uriHost.equals("")) {
            this.uriHost = null;
        }
        this.initialize(this.fs.getConf());
    }
    
    @Override
    public void initialize(final Configuration conf) throws IOException {
        Preconditions.checkNotNull((Object)conf);
        int maxRecords = conf.getInt("fs.metadatastore.local.max_records", 128);
        if (maxRecords < 4) {
            maxRecords = 4;
        }
        this.fileHash = new LruHashMap<Path, PathMetadata>(maxRecords / 2, maxRecords);
        this.dirHash = new LruHashMap<Path, DirListingMetadata>(maxRecords / 4, maxRecords);
    }
    
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("LocalMetadataStore{");
        sb.append(", uriHost='").append(this.uriHost).append('\'');
        sb.append('}');
        return sb.toString();
    }
    
    @Override
    public void delete(final Path p) throws IOException {
        this.doDelete(p, false, true);
    }
    
    @Override
    public void forgetMetadata(final Path p) throws IOException {
        this.doDelete(p, false, false);
    }
    
    @Override
    public void deleteSubtree(final Path path) throws IOException {
        this.doDelete(path, true, true);
    }
    
    private synchronized void doDelete(final Path p, final boolean recursive, final boolean tombstone) {
        final Path path = this.standardize(p);
        this.deleteHashEntries(path, tombstone);
        if (recursive) {
            deleteHashByAncestor(path, this.dirHash, tombstone);
            deleteHashByAncestor(path, this.fileHash, tombstone);
        }
    }
    
    @Override
    public synchronized PathMetadata get(final Path p) throws IOException {
        return this.get(p, false);
    }
    
    @Override
    public PathMetadata get(final Path p, final boolean wantEmptyDirectoryFlag) throws IOException {
        final Path path = this.standardize(p);
        synchronized (this) {
            final PathMetadata m = this.fileHash.mruGet(path);
            if (wantEmptyDirectoryFlag && m != null && m.getFileStatus().isDirectory()) {
                m.setIsEmptyDirectory(this.isEmptyDirectory(p));
            }
            LocalMetadataStore.LOG.debug("get({}) -> {}", (Object)path, (Object)((m == null) ? "null" : m.prettyPrint()));
            return m;
        }
    }
    
    private Tristate isEmptyDirectory(final Path p) {
        final DirListingMetadata dirMeta = this.dirHash.get(p);
        return dirMeta.withoutTombstones().isEmpty();
    }
    
    @Override
    public synchronized DirListingMetadata listChildren(final Path p) throws IOException {
        final Path path = this.standardize(p);
        final DirListingMetadata listing = this.dirHash.mruGet(path);
        if (LocalMetadataStore.LOG.isDebugEnabled()) {
            LocalMetadataStore.LOG.debug("listChildren({}) -> {}", (Object)path, (Object)((listing == null) ? "null" : listing.prettyPrint()));
        }
        return (listing == null) ? null : new DirListingMetadata(listing);
    }
    
    @Override
    public void move(final Collection<Path> pathsToDelete, final Collection<PathMetadata> pathsToCreate) throws IOException {
        Preconditions.checkNotNull((Object)pathsToDelete, (Object)"pathsToDelete is null");
        Preconditions.checkNotNull((Object)pathsToCreate, (Object)"pathsToCreate is null");
        Preconditions.checkArgument(pathsToDelete.size() == pathsToCreate.size(), (Object)"Must supply same number of paths to delete/create.");
        synchronized (this) {
            for (final Path meta : pathsToDelete) {
                LocalMetadataStore.LOG.debug("move: deleting metadata {}", (Object)meta);
                this.delete(meta);
            }
            for (final PathMetadata meta2 : pathsToCreate) {
                LocalMetadataStore.LOG.debug("move: adding metadata {}", (Object)meta2);
                this.put(meta2);
            }
            for (final PathMetadata meta2 : pathsToCreate) {
                final FileStatus status = meta2.getFileStatus();
                if (status != null) {
                    if (status.isDirectory()) {
                        continue;
                    }
                    final DirListingMetadata dir = this.listChildren(status.getPath());
                    if (dir == null) {
                        continue;
                    }
                    dir.setAuthoritative(true);
                }
            }
        }
    }
    
    @Override
    public void put(final PathMetadata meta) throws IOException {
        Preconditions.checkNotNull((Object)meta);
        final FileStatus status = meta.getFileStatus();
        final Path path = this.standardize(status.getPath());
        synchronized (this) {
            if (LocalMetadataStore.LOG.isDebugEnabled()) {
                LocalMetadataStore.LOG.debug("put {} -> {}", (Object)path, (Object)meta.prettyPrint());
            }
            this.fileHash.put(path, meta);
            if (status.isDirectory()) {
                final DirListingMetadata dir = this.dirHash.mruGet(path);
                if (dir == null) {
                    this.dirHash.put(path, new DirListingMetadata(path, DirListingMetadata.EMPTY_DIR, false));
                }
            }
            final Path parentPath = path.getParent();
            if (parentPath != null) {
                DirListingMetadata parent = this.dirHash.mruGet(parentPath);
                if (parent == null) {
                    parent = new DirListingMetadata(parentPath, DirListingMetadata.EMPTY_DIR, false);
                    this.dirHash.put(parentPath, parent);
                }
                parent.put(status);
            }
        }
    }
    
    @Override
    public synchronized void put(final DirListingMetadata meta) throws IOException {
        if (LocalMetadataStore.LOG.isDebugEnabled()) {
            LocalMetadataStore.LOG.debug("put dirMeta {}", (Object)meta.prettyPrint());
        }
        this.dirHash.put(this.standardize(meta.getPath()), meta);
    }
    
    @Override
    public void close() throws IOException {
    }
    
    @Override
    public void destroy() throws IOException {
        if (this.dirHash != null) {
            this.dirHash.clear();
        }
    }
    
    @Override
    public synchronized void prune(final long modTime) throws IOException {
        final Iterator<Map.Entry<Path, PathMetadata>> files = (Iterator<Map.Entry<Path, PathMetadata>>)this.fileHash.entrySet().iterator();
        while (files.hasNext()) {
            final Map.Entry<Path, PathMetadata> entry = files.next();
            if (this.expired(entry.getValue().getFileStatus(), modTime)) {
                files.remove();
            }
        }
        final Iterator<Map.Entry<Path, DirListingMetadata>> dirs = (Iterator<Map.Entry<Path, DirListingMetadata>>)this.dirHash.entrySet().iterator();
        final Collection<Path> ancestors = new LinkedList<Path>();
        while (dirs.hasNext()) {
            final Map.Entry<Path, DirListingMetadata> entry2 = dirs.next();
            final Path path = entry2.getKey();
            final DirListingMetadata metadata = entry2.getValue();
            final Collection<PathMetadata> oldChildren = metadata.getListing();
            final Collection<PathMetadata> newChildren = new LinkedList<PathMetadata>();
            for (final PathMetadata child : oldChildren) {
                final FileStatus status = child.getFileStatus();
                if (!this.expired(status, modTime)) {
                    newChildren.add(child);
                }
            }
            if (newChildren.size() == 0) {
                dirs.remove();
                ancestors.add(entry2.getKey());
            }
            else {
                this.dirHash.put(path, new DirListingMetadata(path, newChildren, false));
            }
        }
    }
    
    private boolean expired(final FileStatus status, final long expiry) {
        return status.getModificationTime() < expiry && !status.isDirectory();
    }
    
    @VisibleForTesting
    static <T> void deleteHashByAncestor(final Path ancestor, final Map<Path, T> hash, final boolean tombstone) {
        final Iterator<Map.Entry<Path, T>> it = hash.entrySet().iterator();
        while (it.hasNext()) {
            final Map.Entry<Path, T> entry = it.next();
            final Path f = entry.getKey();
            final T meta = entry.getValue();
            if (isAncestorOf(ancestor, f)) {
                if (tombstone) {
                    if (meta instanceof PathMetadata) {
                        entry.setValue((T)PathMetadata.tombstone(f));
                    }
                    else {
                        if (!(meta instanceof DirListingMetadata)) {
                            throw new IllegalStateException("Unknown type in hash");
                        }
                        it.remove();
                    }
                }
                else {
                    it.remove();
                }
            }
        }
    }
    
    private static boolean isAncestorOf(final Path ancestor, final Path f) {
        String aStr = ancestor.toString();
        if (!ancestor.isRoot()) {
            aStr += "/";
        }
        final String fStr = f.toString();
        return fStr.startsWith(aStr);
    }
    
    private void deleteHashEntries(final Path path, final boolean tombstone) {
        LocalMetadataStore.LOG.debug("delete file entry for {}", (Object)path);
        if (tombstone) {
            this.fileHash.put(path, PathMetadata.tombstone(path));
        }
        else {
            this.fileHash.remove(path);
        }
        LocalMetadataStore.LOG.debug("removing listing of {}", (Object)path);
        this.dirHash.remove(path);
        final Path parent = path.getParent();
        if (parent != null) {
            final DirListingMetadata dir = this.dirHash.get(parent);
            if (dir != null) {
                LocalMetadataStore.LOG.debug("removing parent's entry for {} ", (Object)path);
                if (tombstone) {
                    dir.markDeleted(path);
                }
                else {
                    dir.remove(path);
                }
            }
        }
    }
    
    private Path standardize(final Path p) {
        Preconditions.checkArgument(p.isAbsolute(), (Object)"Path must be absolute");
        final URI uri = p.toUri();
        if (this.uriHost != null) {
            Preconditions.checkArgument(!isEmpty(uri.getHost()));
        }
        return p;
    }
    
    private static boolean isEmpty(final String s) {
        return s == null || s.isEmpty();
    }
    
    static {
        LOG = LoggerFactory.getLogger((Class)MetadataStore.class);
    }
}
