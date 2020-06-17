// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3;

import java.util.Set;
import java.io.File;
import org.apache.hadoop.fs.Path;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import java.net.URI;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceAudience;

@InterfaceAudience.Private
@InterfaceStability.Unstable
@Deprecated
public interface FileSystemStore
{
    void initialize(final URI p0, final Configuration p1) throws IOException;
    
    String getVersion() throws IOException;
    
    void storeINode(final Path p0, final INode p1) throws IOException;
    
    void storeBlock(final Block p0, final File p1) throws IOException;
    
    boolean inodeExists(final Path p0) throws IOException;
    
    boolean blockExists(final long p0) throws IOException;
    
    INode retrieveINode(final Path p0) throws IOException;
    
    File retrieveBlock(final Block p0, final long p1) throws IOException;
    
    void deleteINode(final Path p0) throws IOException;
    
    void deleteBlock(final Block p0) throws IOException;
    
    Set<Path> listSubPaths(final Path p0) throws IOException;
    
    Set<Path> listDeepSubPaths(final Path p0) throws IOException;
    
    void purge() throws IOException;
    
    void dump() throws IOException;
}
