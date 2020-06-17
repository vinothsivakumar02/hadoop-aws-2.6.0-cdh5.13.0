// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3a.s3guard;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.Tristate;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceAudience;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class PathMetadata
{
    private final FileStatus fileStatus;
    private Tristate isEmptyDirectory;
    private boolean isDeleted;
    
    public static PathMetadata tombstone(final Path path) {
        final long now = System.currentTimeMillis();
        final FileStatus status = new FileStatus(0L, false, 0, 0L, now, path);
        return new PathMetadata(status, Tristate.UNKNOWN, true);
    }
    
    public PathMetadata(final FileStatus fileStatus) {
        this(fileStatus, Tristate.UNKNOWN);
    }
    
    public PathMetadata(final FileStatus fileStatus, final Tristate isEmptyDir) {
        this(fileStatus, isEmptyDir, false);
    }
    
    public PathMetadata(final FileStatus fileStatus, final Tristate isEmptyDir, final boolean isDeleted) {
        Preconditions.checkNotNull((Object)fileStatus, (Object)"fileStatus must be non-null");
        Preconditions.checkNotNull((Object)fileStatus.getPath(), (Object)"fileStatus path must be non-null");
        Preconditions.checkArgument(fileStatus.getPath().isAbsolute(), (Object)"path must be absolute");
        this.fileStatus = fileStatus;
        this.isEmptyDirectory = isEmptyDir;
        this.isDeleted = isDeleted;
    }
    
    public final FileStatus getFileStatus() {
        return this.fileStatus;
    }
    
    public Tristate isEmptyDirectory() {
        return this.isEmptyDirectory;
    }
    
    void setIsEmptyDirectory(final Tristate isEmptyDirectory) {
        this.isEmptyDirectory = isEmptyDirectory;
    }
    
    public boolean isDeleted() {
        return this.isDeleted;
    }
    
    void setIsDeleted(final boolean isDeleted) {
        this.isDeleted = isDeleted;
    }
    
    @Override
    public boolean equals(final Object o) {
        return o instanceof PathMetadata && this.fileStatus.equals((Object)((PathMetadata)o).fileStatus);
    }
    
    @Override
    public int hashCode() {
        return this.fileStatus.hashCode();
    }
    
    @Override
    public String toString() {
        return "PathMetadata{fileStatus=" + this.fileStatus + "; isEmptyDirectory=" + this.isEmptyDirectory + "; isDeleted=" + this.isDeleted + '}';
    }
    
    public void prettyPrint(final StringBuilder sb) {
        sb.append(String.format("%-5s %-20s %-7d %-8s %-6s", this.fileStatus.isDirectory() ? "dir" : "file", this.fileStatus.getPath().toString(), this.fileStatus.getLen(), this.isEmptyDirectory.name(), this.isDeleted));
        sb.append(this.fileStatus);
    }
    
    public String prettyPrint() {
        final StringBuilder sb = new StringBuilder();
        this.prettyPrint(sb);
        return sb.toString();
    }
}
