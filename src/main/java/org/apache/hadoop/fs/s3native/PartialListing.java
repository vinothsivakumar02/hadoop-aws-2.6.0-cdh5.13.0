// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3native;

import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceAudience;

@InterfaceAudience.Private
@InterfaceStability.Unstable
class PartialListing
{
    private final String priorLastKey;
    private final FileMetadata[] files;
    private final String[] commonPrefixes;
    
    public PartialListing(final String priorLastKey, final FileMetadata[] files, final String[] commonPrefixes) {
        this.priorLastKey = priorLastKey;
        this.files = files;
        this.commonPrefixes = commonPrefixes;
    }
    
    public FileMetadata[] getFiles() {
        return this.files;
    }
    
    public String[] getCommonPrefixes() {
        return this.commonPrefixes;
    }
    
    public String getPriorLastKey() {
        return this.priorLastKey;
    }
}
