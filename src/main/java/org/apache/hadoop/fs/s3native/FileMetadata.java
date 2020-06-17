// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3native;

import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceAudience;

@InterfaceAudience.Private
@InterfaceStability.Unstable
class FileMetadata
{
    private final String key;
    private final long length;
    private final long lastModified;
    
    public FileMetadata(final String key, final long length, final long lastModified) {
        this.key = key;
        this.length = length;
        this.lastModified = lastModified;
    }
    
    public String getKey() {
        return this.key;
    }
    
    public long getLength() {
        return this.length;
    }
    
    public long getLastModified() {
        return this.lastModified;
    }
    
    @Override
    public String toString() {
        return "FileMetadata[" + this.key + ", " + this.length + ", " + this.lastModified + "]";
    }
}
