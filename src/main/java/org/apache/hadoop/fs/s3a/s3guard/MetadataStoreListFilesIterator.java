// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3a.s3guard;

import org.slf4j.LoggerFactory;
import java.util.Queue;
import java.util.Collection;
import java.util.ArrayList;
import java.util.LinkedList;
import java.io.IOException;
import com.google.common.base.Preconditions;
import java.util.HashSet;
import java.util.Iterator;
import org.apache.hadoop.fs.Path;
import java.util.Set;
import org.slf4j.Logger;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.RemoteIterator;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class MetadataStoreListFilesIterator implements RemoteIterator<FileStatus>
{
    public static final Logger LOG;
    private final boolean allowAuthoritative;
    private final MetadataStore metadataStore;
    private final Set<Path> tombstones;
    private Iterator<FileStatus> leafNodesIterator;
    
    public MetadataStoreListFilesIterator(final MetadataStore ms, final PathMetadata meta, final boolean allowAuthoritative) throws IOException {
        this.tombstones = new HashSet<Path>();
        this.leafNodesIterator = null;
        Preconditions.checkNotNull((Object)ms);
        this.metadataStore = ms;
        this.allowAuthoritative = allowAuthoritative;
        this.prefetch(meta);
    }
    
    private void prefetch(final PathMetadata meta) throws IOException {
        final Queue<PathMetadata> queue = new LinkedList<PathMetadata>();
        final Collection<FileStatus> leafNodes = new ArrayList<FileStatus>();
        if (meta != null) {
            final Path path = meta.getFileStatus().getPath();
            if (path.isRoot()) {
                final DirListingMetadata rootListing = this.metadataStore.listChildren(path);
                if (rootListing != null) {
                    this.tombstones.addAll(rootListing.listTombstones());
                    queue.addAll(rootListing.withoutTombstones().getListing());
                }
            }
            else {
                queue.add(meta);
            }
        }
        while (!queue.isEmpty()) {
            final PathMetadata nextMetadata = queue.poll();
            final FileStatus nextStatus = nextMetadata.getFileStatus();
            if (nextStatus.isFile()) {
                leafNodes.add(nextStatus);
            }
            else {
                if (!nextStatus.isDirectory()) {
                    continue;
                }
                final Path path2 = nextStatus.getPath();
                final DirListingMetadata children = this.metadataStore.listChildren(path2);
                if (children == null) {
                    continue;
                }
                this.tombstones.addAll(children.listTombstones());
                final Collection<PathMetadata> liveChildren = children.withoutTombstones().getListing();
                if (!liveChildren.isEmpty()) {
                    queue.addAll(liveChildren);
                }
                else {
                    if (!this.allowAuthoritative || !children.isAuthoritative()) {
                        continue;
                    }
                    leafNodes.add(nextStatus);
                }
            }
        }
        this.leafNodesIterator = leafNodes.iterator();
    }
    
    public boolean hasNext() {
        return this.leafNodesIterator.hasNext();
    }
    
    public FileStatus next() {
        return this.leafNodesIterator.next();
    }
    
    public Set<Path> listTombstones() {
        return this.tombstones;
    }
    
    static {
        LOG = LoggerFactory.getLogger((Class)MetadataStoreListFilesIterator.class);
    }
}
