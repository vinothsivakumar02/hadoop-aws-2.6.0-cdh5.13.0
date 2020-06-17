// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3a.s3guard;

import java.util.NoSuchElementException;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import java.util.Collection;
import com.google.common.base.Preconditions;
import java.util.LinkedList;
import java.util.Queue;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.RemoteIterator;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DescendantsIterator implements RemoteIterator<FileStatus>
{
    private final MetadataStore metadataStore;
    private final Queue<PathMetadata> queue;
    
    public DescendantsIterator(final MetadataStore ms, final PathMetadata meta) throws IOException {
        this.queue = new LinkedList<PathMetadata>();
        Preconditions.checkNotNull((Object)ms);
        this.metadataStore = ms;
        if (meta != null) {
            final Path path = meta.getFileStatus().getPath();
            if (path.isRoot()) {
                DirListingMetadata rootListing = ms.listChildren(path);
                if (rootListing != null) {
                    rootListing = rootListing.withoutTombstones();
                    this.queue.addAll(rootListing.getListing());
                }
            }
            else {
                this.queue.add(meta);
            }
        }
    }
    
    public boolean hasNext() throws IOException {
        return !this.queue.isEmpty();
    }
    
    public FileStatus next() throws IOException {
        if (!this.hasNext()) {
            throw new NoSuchElementException("No more descendants.");
        }
        final PathMetadata next = this.queue.poll();
        if (next.getFileStatus().isDirectory()) {
            final Path path = next.getFileStatus().getPath();
            final DirListingMetadata meta = this.metadataStore.listChildren(path);
            if (meta != null) {
                final Collection<PathMetadata> more = meta.withoutTombstones().getListing();
                if (!more.isEmpty()) {
                    this.queue.addAll(more);
                }
            }
        }
        return next.getFileStatus();
    }
}
