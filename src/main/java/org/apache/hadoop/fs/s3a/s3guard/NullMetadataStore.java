// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3a.s3guard;

import java.util.Collection;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;

public class NullMetadataStore implements MetadataStore
{
    @Override
    public void initialize(final FileSystem fs) throws IOException {
    }
    
    @Override
    public void initialize(final Configuration conf) throws IOException {
    }
    
    @Override
    public void close() throws IOException {
    }
    
    @Override
    public void delete(final Path path) throws IOException {
    }
    
    @Override
    public void forgetMetadata(final Path path) throws IOException {
    }
    
    @Override
    public void deleteSubtree(final Path path) throws IOException {
    }
    
    @Override
    public PathMetadata get(final Path path) throws IOException {
        return null;
    }
    
    @Override
    public PathMetadata get(final Path path, final boolean wantEmptyDirectoryFlag) throws IOException {
        return null;
    }
    
    @Override
    public DirListingMetadata listChildren(final Path path) throws IOException {
        return null;
    }
    
    @Override
    public void move(final Collection<Path> pathsToDelete, final Collection<PathMetadata> pathsToCreate) throws IOException {
    }
    
    @Override
    public void put(final PathMetadata meta) throws IOException {
    }
    
    @Override
    public void put(final DirListingMetadata meta) throws IOException {
    }
    
    @Override
    public void destroy() throws IOException {
    }
    
    @Override
    public void prune(final long modTime) {
    }
    
    @Override
    public String toString() {
        return "NullMetadataStore";
    }
}
