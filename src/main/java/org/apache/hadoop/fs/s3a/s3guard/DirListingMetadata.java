// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3a.s3guard;

import java.net.URISyntaxException;
import java.net.URI;
import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.s3a.Tristate;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import org.apache.hadoop.fs.Path;
import java.util.Collection;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceAudience;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DirListingMetadata
{
    public static final Collection<PathMetadata> EMPTY_DIR;
    private final Path path;
    private Map<Path, PathMetadata> listMap;
    private boolean isAuthoritative;
    
    public DirListingMetadata(final Path path, final Collection<PathMetadata> listing, final boolean isAuthoritative) {
        this.listMap = new ConcurrentHashMap<Path, PathMetadata>();
        this.checkPathAbsolute(path);
        this.path = path;
        if (listing != null) {
            for (final PathMetadata entry : listing) {
                final Path childPath = entry.getFileStatus().getPath();
                this.checkChildPath(childPath);
                this.listMap.put(childPath, entry);
            }
        }
        this.isAuthoritative = isAuthoritative;
    }
    
    public DirListingMetadata(final DirListingMetadata d) {
        this.listMap = new ConcurrentHashMap<Path, PathMetadata>();
        this.path = d.path;
        this.isAuthoritative = d.isAuthoritative;
        this.listMap = new ConcurrentHashMap<Path, PathMetadata>(d.listMap);
    }
    
    public Path getPath() {
        return this.path;
    }
    
    public Collection<PathMetadata> getListing() {
        return Collections.unmodifiableCollection((Collection<? extends PathMetadata>)this.listMap.values());
    }
    
    public Set<Path> listTombstones() {
        final Set<Path> tombstones = new HashSet<Path>();
        for (final PathMetadata meta : this.listMap.values()) {
            if (meta.isDeleted()) {
                tombstones.add(meta.getFileStatus().getPath());
            }
        }
        return tombstones;
    }
    
    public DirListingMetadata withoutTombstones() {
        final Collection<PathMetadata> filteredList = new ArrayList<PathMetadata>();
        for (final PathMetadata meta : this.listMap.values()) {
            if (!meta.isDeleted()) {
                filteredList.add(meta);
            }
        }
        return new DirListingMetadata(this.path, filteredList, this.isAuthoritative);
    }
    
    public int numEntries() {
        return this.listMap.size();
    }
    
    public boolean isAuthoritative() {
        return this.isAuthoritative;
    }
    
    public Tristate isEmpty() {
        if (!this.getListing().isEmpty()) {
            return Tristate.FALSE;
        }
        if (this.isAuthoritative()) {
            return Tristate.TRUE;
        }
        return Tristate.UNKNOWN;
    }
    
    public void setAuthoritative(final boolean authoritative) {
        this.isAuthoritative = authoritative;
    }
    
    public PathMetadata get(final Path childPath) {
        this.checkChildPath(childPath);
        return this.listMap.get(childPath);
    }
    
    public void markDeleted(final Path childPath) {
        this.checkChildPath(childPath);
        this.listMap.put(childPath, PathMetadata.tombstone(childPath));
    }
    
    public void remove(final Path childPath) {
        this.checkChildPath(childPath);
        this.listMap.remove(childPath);
    }
    
    public boolean put(final FileStatus childFileStatus) {
        Preconditions.checkNotNull((Object)childFileStatus, (Object)"childFileStatus must be non-null");
        final Path childPath = this.childStatusToPathKey(childFileStatus);
        final PathMetadata newValue = new PathMetadata(childFileStatus);
        final PathMetadata oldValue = this.listMap.put(childPath, newValue);
        return oldValue == null || !oldValue.equals(newValue);
    }
    
    @Override
    public String toString() {
        return "DirListingMetadata{path=" + this.path + ", listMap=" + this.listMap + ", isAuthoritative=" + this.isAuthoritative + '}';
    }
    
    public void prettyPrint(final StringBuilder sb) {
        sb.append(String.format("DirMeta %-20s %-18s", this.path.toString(), this.isAuthoritative ? "Authoritative" : "Not Authoritative"));
        for (final Map.Entry<Path, PathMetadata> entry : this.listMap.entrySet()) {
            sb.append("\n   key: ").append(entry.getKey()).append(": ");
            entry.getValue().prettyPrint(sb);
        }
        sb.append("\n");
    }
    
    public String prettyPrint() {
        final StringBuilder sb = new StringBuilder();
        this.prettyPrint(sb);
        return sb.toString();
    }
    
    private void checkChildPath(final Path childPath) {
        this.checkPathAbsolute(childPath);
        final URI parentUri = this.path.toUri();
        if (parentUri.getHost() != null) {
            final URI childUri = childPath.toUri();
            Preconditions.checkNotNull((Object)childUri.getHost(), "Expected non-null URI host: %s", new Object[] { childUri });
            Preconditions.checkArgument(childUri.getHost().equals(parentUri.getHost()), "childUri %s and parentUri %s should have the same host", new Object[] { childUri, parentUri });
            Preconditions.checkNotNull((Object)childUri.getScheme());
        }
        Preconditions.checkArgument(!childPath.isRoot(), (Object)"childPath cannot be the root path");
        Preconditions.checkArgument(childPath.getParent().equals((Object)this.path), "childPath %s must be a child of %s", new Object[] { childPath, this.path });
    }
    
    private Path childStatusToPathKey(final FileStatus status) {
        final Path p = status.getPath();
        Preconditions.checkNotNull((Object)p, (Object)"Child status' path cannot be null");
        Preconditions.checkArgument(!p.isRoot(), (Object)"childPath cannot be the root path");
        Preconditions.checkArgument(p.getParent().equals((Object)this.path), (Object)"childPath must be a child of path");
        final URI uri = p.toUri();
        final URI parentUri = this.path.toUri();
        if (uri.getHost() == null && parentUri.getHost() != null) {
            try {
                return new Path(new URI(parentUri.getScheme(), parentUri.getHost(), uri.getPath(), uri.getFragment()));
            }
            catch (URISyntaxException e) {
                throw new IllegalArgumentException("FileStatus path invalid with  added " + parentUri.getScheme() + "://" + parentUri.getHost() + " added", e);
            }
        }
        return p;
    }
    
    private void checkPathAbsolute(final Path p) {
        Preconditions.checkNotNull((Object)p, (Object)"path must be non-null");
        Preconditions.checkArgument(p.isAbsolute(), (Object)"path must be absolute");
    }
    
    static {
        EMPTY_DIR = Collections.emptyList();
    }
}
