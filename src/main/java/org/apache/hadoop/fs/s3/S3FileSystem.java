// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3;

import java.io.InputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import java.io.OutputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.util.Progressable;
import java.util.Iterator;
import org.apache.hadoop.fs.FileStatus;
import java.io.FileNotFoundException;
import java.util.List;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import java.util.ArrayList;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.retry.RetryProxy;
import java.util.Map;
import org.apache.hadoop.io.retry.RetryPolicy;
import java.util.HashMap;
import org.apache.hadoop.io.retry.RetryPolicies;
import java.util.concurrent.TimeUnit;
import java.io.IOException;
import org.apache.hadoop.fs.s3native.S3xLoginHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import java.net.URI;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.FileSystem;

@InterfaceAudience.Public
@InterfaceStability.Stable
@Deprecated
public class S3FileSystem extends FileSystem
{
    private static final AtomicBoolean hasWarnedDeprecation;
    private URI uri;
    private FileSystemStore store;
    private Path workingDir;
    
    public S3FileSystem() {
    }
    
    public S3FileSystem(final FileSystemStore store) {
        this.store = store;
    }
    
    private static void warnDeprecation() {
        if (!S3FileSystem.hasWarnedDeprecation.getAndSet(true)) {
            S3FileSystem.LOG.warn((Object)"S3FileSystem is deprecated and will be removed in future releases. Use NativeS3FileSystem or S3AFileSystem instead.");
        }
    }
    
    public String getScheme() {
        return "s3";
    }
    
    public URI getUri() {
        return this.uri;
    }
    
    public void initialize(final URI uri, final Configuration conf) throws IOException {
        super.initialize(uri, conf);
        warnDeprecation();
        if (this.store == null) {
            this.store = createDefaultStore(conf);
        }
        this.store.initialize(uri, conf);
        this.setConf(conf);
        this.uri = S3xLoginHelper.buildFSURI(uri);
        this.workingDir = new Path("/user", System.getProperty("user.name")).makeQualified((FileSystem)this);
    }
    
    private static FileSystemStore createDefaultStore(final Configuration conf) {
        final FileSystemStore store = new Jets3tFileSystemStore();
        final RetryPolicy basePolicy = RetryPolicies.retryUpToMaximumCountWithFixedSleep(conf.getInt("fs.s3.maxRetries", 4), conf.getLong("fs.s3.sleepTimeSeconds", 10L), TimeUnit.SECONDS);
        final Map<Class<? extends Exception>, RetryPolicy> exceptionToPolicyMap = new HashMap<Class<? extends Exception>, RetryPolicy>();
        exceptionToPolicyMap.put(IOException.class, basePolicy);
        exceptionToPolicyMap.put(S3Exception.class, basePolicy);
        final RetryPolicy methodPolicy = RetryPolicies.retryByException(RetryPolicies.TRY_ONCE_THEN_FAIL, (Map)exceptionToPolicyMap);
        final Map<String, RetryPolicy> methodNameToPolicyMap = new HashMap<String, RetryPolicy>();
        methodNameToPolicyMap.put("storeBlock", methodPolicy);
        methodNameToPolicyMap.put("retrieveBlock", methodPolicy);
        return (FileSystemStore)RetryProxy.create((Class)FileSystemStore.class, (Object)store, (Map)methodNameToPolicyMap);
    }
    
    public Path getWorkingDirectory() {
        return this.workingDir;
    }
    
    public void setWorkingDirectory(final Path dir) {
        this.workingDir = this.makeAbsolute(dir);
    }
    
    private Path makeAbsolute(final Path path) {
        if (path.isAbsolute()) {
            return path;
        }
        return new Path(this.workingDir, path);
    }
    
    protected void checkPath(final Path path) {
        S3xLoginHelper.checkPath(this.getConf(), this.getUri(), path, this.getDefaultPort());
    }
    
    protected URI canonicalizeUri(final URI rawUri) {
        return S3xLoginHelper.canonicalizeUri(rawUri, this.getDefaultPort());
    }
    
    public boolean mkdirs(final Path path, final FsPermission permission) throws IOException {
        Path absolutePath = this.makeAbsolute(path);
        final List<Path> paths = new ArrayList<Path>();
        do {
            paths.add(0, absolutePath);
            absolutePath = absolutePath.getParent();
        } while (absolutePath != null);
        boolean result = true;
        for (int i = 0; i < paths.size(); ++i) {
            final Path p = paths.get(i);
            try {
                result &= this.mkdir(p);
            }
            catch (FileAlreadyExistsException e) {
                if (i + 1 < paths.size()) {
                    throw new ParentNotDirectoryException(e.getMessage());
                }
                throw e;
            }
        }
        return result;
    }
    
    private boolean mkdir(final Path path) throws IOException {
        final Path absolutePath = this.makeAbsolute(path);
        final INode inode = this.store.retrieveINode(absolutePath);
        if (inode == null) {
            this.store.storeINode(absolutePath, INode.DIRECTORY_INODE);
        }
        else if (inode.isFile()) {
            throw new FileAlreadyExistsException(String.format("Can't make directory for path %s since it is a file.", absolutePath));
        }
        return true;
    }
    
    public boolean isFile(final Path path) throws IOException {
        final INode inode = this.store.retrieveINode(this.makeAbsolute(path));
        return inode != null && inode.isFile();
    }
    
    private INode checkFile(final Path path) throws IOException {
        final INode inode = this.store.retrieveINode(this.makeAbsolute(path));
        final String message = String.format("No such file: '%s'", path.toString());
        if (inode == null) {
            throw new FileNotFoundException(message + " does not exist");
        }
        if (inode.isDirectory()) {
            throw new FileNotFoundException(message + " is a directory");
        }
        return inode;
    }
    
    public FileStatus[] listStatus(final Path f) throws IOException {
        final Path absolutePath = this.makeAbsolute(f);
        final INode inode = this.store.retrieveINode(absolutePath);
        if (inode == null) {
            throw new FileNotFoundException("File " + f + " does not exist.");
        }
        if (inode.isFile()) {
            return new FileStatus[] { new S3FileStatus(f.makeQualified((FileSystem)this), inode) };
        }
        final ArrayList<FileStatus> ret = new ArrayList<FileStatus>();
        for (final Path p : this.store.listSubPaths(absolutePath)) {
            ret.add(this.getFileStatus(p.makeQualified((FileSystem)this)));
        }
        return ret.toArray(new FileStatus[0]);
    }
    
    public FSDataOutputStream append(final Path f, final int bufferSize, final Progressable progress) throws IOException {
        throw new IOException("Not supported");
    }
    
    public FSDataOutputStream create(final Path file, final FsPermission permission, final boolean overwrite, final int bufferSize, final short replication, final long blockSize, final Progressable progress) throws IOException {
        final INode inode = this.store.retrieveINode(this.makeAbsolute(file));
        if (inode != null) {
            if (!overwrite || inode.isDirectory()) {
                String message = String.format("File already exists: '%s'", file);
                if (inode.isDirectory()) {
                    message += " is a directory";
                }
                throw new FileAlreadyExistsException(message);
            }
            this.delete(file, true);
        }
        else {
            final Path parent = file.getParent();
            if (parent != null && !this.mkdirs(parent)) {
                throw new IOException("Mkdirs failed to create " + parent.toString());
            }
        }
        return new FSDataOutputStream((OutputStream)new S3OutputStream(this.getConf(), this.store, this.makeAbsolute(file), blockSize, progress, bufferSize), this.statistics);
    }
    
    public FSDataInputStream open(final Path path, final int bufferSize) throws IOException {
        final INode inode = this.checkFile(path);
        return new FSDataInputStream((InputStream)new S3InputStream(this.getConf(), this.store, inode, this.statistics));
    }
    
    public boolean rename(final Path src, final Path dst) throws IOException {
        final Path absoluteSrc = this.makeAbsolute(src);
        final INode srcINode = this.store.retrieveINode(absoluteSrc);
        if (srcINode == null) {
            return false;
        }
        Path absoluteDst = this.makeAbsolute(dst);
        INode dstINode = this.store.retrieveINode(absoluteDst);
        if (dstINode != null && dstINode.isDirectory()) {
            absoluteDst = new Path(absoluteDst, absoluteSrc.getName());
            dstINode = this.store.retrieveINode(absoluteDst);
        }
        if (dstINode != null) {
            return false;
        }
        final Path dstParent = absoluteDst.getParent();
        if (dstParent != null) {
            final INode dstParentINode = this.store.retrieveINode(dstParent);
            if (dstParentINode == null || dstParentINode.isFile()) {
                return false;
            }
        }
        return this.renameRecursive(absoluteSrc, absoluteDst);
    }
    
    private boolean renameRecursive(final Path src, final Path dst) throws IOException {
        final INode srcINode = this.store.retrieveINode(src);
        this.store.storeINode(dst, srcINode);
        this.store.deleteINode(src);
        if (srcINode.isDirectory()) {
            for (final Path oldSrc : this.store.listDeepSubPaths(src)) {
                final INode inode = this.store.retrieveINode(oldSrc);
                if (inode == null) {
                    return false;
                }
                final String oldSrcPath = oldSrc.toUri().getPath();
                final String srcPath = src.toUri().getPath();
                final String dstPath = dst.toUri().getPath();
                final Path newDst = new Path(oldSrcPath.replaceFirst(srcPath, dstPath));
                this.store.storeINode(newDst, inode);
                this.store.deleteINode(oldSrc);
            }
        }
        return true;
    }
    
    public boolean delete(final Path path, final boolean recursive) throws IOException {
        final Path absolutePath = this.makeAbsolute(path);
        final INode inode = this.store.retrieveINode(absolutePath);
        if (inode == null) {
            return false;
        }
        if (inode.isFile()) {
            this.store.deleteINode(absolutePath);
            for (final Block block : inode.getBlocks()) {
                this.store.deleteBlock(block);
            }
        }
        else {
            FileStatus[] contents = null;
            try {
                contents = this.listStatus(absolutePath);
            }
            catch (FileNotFoundException fnfe) {
                return false;
            }
            if (contents.length != 0 && !recursive) {
                throw new IOException("Directory " + path.toString() + " is not empty.");
            }
            for (final FileStatus p : contents) {
                if (!this.delete(p.getPath(), recursive)) {
                    return false;
                }
            }
            this.store.deleteINode(absolutePath);
        }
        return true;
    }
    
    public FileStatus getFileStatus(final Path f) throws IOException {
        final INode inode = this.store.retrieveINode(this.makeAbsolute(f));
        if (inode == null) {
            throw new FileNotFoundException(f + ": No such file or directory.");
        }
        return new S3FileStatus(f.makeQualified((FileSystem)this), inode);
    }
    
    public long getDefaultBlockSize() {
        return this.getConf().getLong("fs.s3.block.size", 67108864L);
    }
    
    public String getCanonicalServiceName() {
        return null;
    }
    
    void dump() throws IOException {
        this.store.dump();
    }
    
    void purge() throws IOException {
        this.store.purge();
    }
    
    static {
        hasWarnedDeprecation = new AtomicBoolean(false);
    }
    
    private static class S3FileStatus extends FileStatus
    {
        S3FileStatus(final Path f, final INode inode) throws IOException {
            super(findLength(inode), inode.isDirectory(), 1, findBlocksize(inode), 0L, f);
        }
        
        private static long findLength(final INode inode) {
            if (!inode.isDirectory()) {
                long length = 0L;
                for (final Block block : inode.getBlocks()) {
                    length += block.getLength();
                }
                return length;
            }
            return 0L;
        }
        
        private static long findBlocksize(final INode inode) {
            final Block[] ret = inode.getBlocks();
            return (ret == null) ? 0L : ret[0].getLength();
        }
    }
}
