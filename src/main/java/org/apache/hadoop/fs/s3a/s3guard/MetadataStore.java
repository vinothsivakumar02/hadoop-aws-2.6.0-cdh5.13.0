// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3a.s3guard;

import java.util.Collection;
import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceAudience;
import java.io.Closeable;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface MetadataStore extends Closeable
{
    void initialize(final FileSystem p0) throws IOException;
    
    void initialize(final Configuration p0) throws IOException;
    
    void delete(final Path p0) throws IOException;
    
    @VisibleForTesting
    void forgetMetadata(final Path p0) throws IOException;
    
    void deleteSubtree(final Path p0) throws IOException;
    
    PathMetadata get(final Path p0) throws IOException;
    
    PathMetadata get(final Path p0, final boolean p1) throws IOException;
    
    DirListingMetadata listChildren(final Path p0) throws IOException;
    
    void move(final Collection<Path> p0, final Collection<PathMetadata> p1) throws IOException;
    
    void put(final PathMetadata p0) throws IOException;
    
    void put(final DirListingMetadata p0) throws IOException;
    
    void destroy() throws IOException;
    
    void prune(final long p0) throws IOException, UnsupportedOperationException;
}
