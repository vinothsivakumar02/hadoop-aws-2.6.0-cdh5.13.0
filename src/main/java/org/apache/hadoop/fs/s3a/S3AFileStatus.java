// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3a;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.FileStatus;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class S3AFileStatus extends FileStatus
{
    private Tristate isEmptyDirectory;
    
    public S3AFileStatus(final boolean isemptydir, final Path path, final String owner) {
        this(Tristate.fromBool(isemptydir), path, owner);
    }
    
    public S3AFileStatus(final Tristate isemptydir, final Path path, final String owner) {
        super(0L, true, 1, 0L, 0L, path);
        this.isEmptyDirectory = isemptydir;
        this.setOwner(owner);
        this.setGroup(owner);
    }
    
    public S3AFileStatus(final long length, final long modification_time, final Path path, final long blockSize, final String owner) {
        super(length, false, 1, blockSize, modification_time, path);
        this.isEmptyDirectory = Tristate.FALSE;
        this.setOwner(owner);
        this.setGroup(owner);
    }
    
    public static S3AFileStatus fromFileStatus(final FileStatus source, final Tristate isEmptyDirectory) {
        if (source.isDirectory()) {
            return new S3AFileStatus(isEmptyDirectory, source.getPath(), source.getOwner());
        }
        return new S3AFileStatus(source.getLen(), source.getModificationTime(), source.getPath(), source.getBlockSize(), source.getOwner());
    }
    
    public Tristate isEmptyDirectory() {
        return this.isEmptyDirectory;
    }
    
    public void setIsEmptyDirectory(final Tristate value) {
        this.isEmptyDirectory = value;
    }
    
    public boolean equals(final Object o) {
        return super.equals(o);
    }
    
    public int hashCode() {
        return super.hashCode();
    }
    
    public long getModificationTime() {
        if (this.isDirectory()) {
            return System.currentTimeMillis();
        }
        return super.getModificationTime();
    }
    
    public String toString() {
        return super.toString() + String.format(" isEmptyDirectory=%s", this.isEmptyDirectory().name());
    }
}
