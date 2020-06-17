// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3native;

import java.io.InputStream;
import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import java.net.URI;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceAudience;

@InterfaceAudience.Private
@InterfaceStability.Unstable
interface NativeFileSystemStore
{
    void initialize(final URI p0, final Configuration p1) throws IOException;
    
    void storeFile(final String p0, final File p1, final byte[] p2) throws IOException;
    
    void storeEmptyFile(final String p0) throws IOException;
    
    FileMetadata retrieveMetadata(final String p0) throws IOException;
    
    InputStream retrieve(final String p0) throws IOException;
    
    InputStream retrieve(final String p0, final long p1) throws IOException;
    
    PartialListing list(final String p0, final int p1) throws IOException;
    
    PartialListing list(final String p0, final int p1, final String p2, final boolean p3) throws IOException;
    
    void delete(final String p0) throws IOException;
    
    void copy(final String p0, final String p1) throws IOException;
    
    void purge(final String p0) throws IOException;
    
    void dump() throws IOException;
}
