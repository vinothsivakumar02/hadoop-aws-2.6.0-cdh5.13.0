// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3native;

import java.security.NoSuchAlgorithmException;
import java.io.BufferedOutputStream;
import java.security.DigestOutputStream;
import java.io.FileOutputStream;
import org.apache.hadoop.fs.LocalDirAllocator;
import java.security.MessageDigest;
import java.io.File;
import java.io.Closeable;
import org.apache.hadoop.io.IOUtils;
import java.io.EOFException;
import com.google.common.base.Preconditions;
import org.slf4j.LoggerFactory;
import java.io.InputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hadoop.fs.FileStatus;
import java.io.FileNotFoundException;
import java.io.OutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.io.retry.RetryProxy;
import java.util.Map;
import org.apache.hadoop.fs.s3.S3Exception;
import org.apache.hadoop.io.retry.RetryPolicy;
import java.util.HashMap;
import org.apache.hadoop.io.retry.RetryPolicies;
import java.util.concurrent.TimeUnit;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import java.net.URI;
import org.slf4j.Logger;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.FileSystem;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class NativeS3FileSystem extends FileSystem
{
    public static final Logger LOG;
    private static final String FOLDER_SUFFIX = "_$folder$";
    static final String PATH_DELIMITER = "/";
    private static final int S3_MAX_LISTING_LENGTH = 1000;
    private URI uri;
    private NativeFileSystemStore store;
    private Path workingDir;
    
    public NativeS3FileSystem() {
    }
    
    public NativeS3FileSystem(final NativeFileSystemStore store) {
        this.store = store;
    }
    
    public String getScheme() {
        return "s3n";
    }
    
    public void initialize(final URI uri, final Configuration conf) throws IOException {
        super.initialize(uri, conf);
        if (this.store == null) {
            this.store = createDefaultStore(conf);
        }
        this.store.initialize(uri, conf);
        this.setConf(conf);
        this.uri = S3xLoginHelper.buildFSURI(uri);
        this.workingDir = new Path("/user", System.getProperty("user.name")).makeQualified(this.uri, this.getWorkingDirectory());
    }
    
    private static NativeFileSystemStore createDefaultStore(final Configuration conf) {
        final NativeFileSystemStore store = new Jets3tNativeFileSystemStore();
        final RetryPolicy basePolicy = RetryPolicies.retryUpToMaximumCountWithFixedSleep(conf.getInt("fs.s3.maxRetries", 4), conf.getLong("fs.s3.sleepTimeSeconds", 10L), TimeUnit.SECONDS);
        final Map<Class<? extends Exception>, RetryPolicy> exceptionToPolicyMap = new HashMap<Class<? extends Exception>, RetryPolicy>();
        exceptionToPolicyMap.put(IOException.class, basePolicy);
        exceptionToPolicyMap.put(S3Exception.class, basePolicy);
        final RetryPolicy methodPolicy = RetryPolicies.retryByException(RetryPolicies.TRY_ONCE_THEN_FAIL, (Map)exceptionToPolicyMap);
        final Map<String, RetryPolicy> methodNameToPolicyMap = new HashMap<String, RetryPolicy>();
        methodNameToPolicyMap.put("storeFile", methodPolicy);
        methodNameToPolicyMap.put("rename", methodPolicy);
        return (NativeFileSystemStore)RetryProxy.create((Class)NativeFileSystemStore.class, (Object)store, (Map)methodNameToPolicyMap);
    }
    
    private static String pathToKey(final Path path) {
        if (path.toUri().getScheme() != null && path.toUri().getPath().isEmpty()) {
            return "";
        }
        if (!path.isAbsolute()) {
            throw new IllegalArgumentException("Path must be absolute: " + path);
        }
        String ret = path.toUri().getPath().substring(1);
        if (ret.endsWith("/") && ret.indexOf("/") != ret.length() - 1) {
            ret = ret.substring(0, ret.length() - 1);
        }
        return ret;
    }
    
    private static Path keyToPath(final String key) {
        return new Path("/" + key);
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
    
    public FSDataOutputStream append(final Path f, final int bufferSize, final Progressable progress) throws IOException {
        throw new IOException("Not supported");
    }
    
    public FSDataOutputStream create(final Path f, final FsPermission permission, final boolean overwrite, final int bufferSize, final short replication, final long blockSize, final Progressable progress) throws IOException {
        if (this.exists(f) && !overwrite) {
            throw new FileAlreadyExistsException("File already exists: " + f);
        }
        if (NativeS3FileSystem.LOG.isDebugEnabled()) {
            NativeS3FileSystem.LOG.debug("Creating new file '" + f + "' in S3");
        }
        final Path absolutePath = this.makeAbsolute(f);
        final String key = pathToKey(absolutePath);
        return new FSDataOutputStream((OutputStream)new NativeS3FsOutputStream(this.getConf(), this.store, key, progress, bufferSize), this.statistics);
    }
    
    public boolean delete(final Path f, final boolean recurse) throws IOException {
        FileStatus status;
        try {
            status = this.getFileStatus(f);
        }
        catch (FileNotFoundException e) {
            if (NativeS3FileSystem.LOG.isDebugEnabled()) {
                NativeS3FileSystem.LOG.debug("Delete called for '" + f + "' but file does not exist, so returning false");
            }
            return false;
        }
        final Path absolutePath = this.makeAbsolute(f);
        final String key = pathToKey(absolutePath);
        if (status.isDirectory()) {
            if (!recurse && this.listStatus(f).length > 0) {
                throw new IOException("Can not delete " + f + " as is a not empty directory and recurse option is false");
            }
            this.createParent(f);
            if (NativeS3FileSystem.LOG.isDebugEnabled()) {
                NativeS3FileSystem.LOG.debug("Deleting directory '" + f + "'");
            }
            String priorLastKey = null;
            do {
                final PartialListing listing = this.store.list(key, 1000, priorLastKey, true);
                for (final FileMetadata file : listing.getFiles()) {
                    this.store.delete(file.getKey());
                }
                priorLastKey = listing.getPriorLastKey();
            } while (priorLastKey != null);
            try {
                this.store.delete(key + "_$folder$");
            }
            catch (FileNotFoundException ex) {}
        }
        else {
            if (NativeS3FileSystem.LOG.isDebugEnabled()) {
                NativeS3FileSystem.LOG.debug("Deleting file '" + f + "'");
            }
            this.createParent(f);
            this.store.delete(key);
        }
        return true;
    }
    
    public FileStatus getFileStatus(final Path f) throws IOException {
        final Path absolutePath = this.makeAbsolute(f);
        final String key = pathToKey(absolutePath);
        if (key.length() == 0) {
            return this.newDirectory(absolutePath);
        }
        if (NativeS3FileSystem.LOG.isDebugEnabled()) {
            NativeS3FileSystem.LOG.debug("getFileStatus retrieving metadata for key '" + key + "'");
        }
        final FileMetadata meta = this.store.retrieveMetadata(key);
        if (meta != null) {
            if (NativeS3FileSystem.LOG.isDebugEnabled()) {
                NativeS3FileSystem.LOG.debug("getFileStatus returning 'file' for key '" + key + "'");
            }
            return this.newFile(meta, absolutePath);
        }
        if (this.store.retrieveMetadata(key + "_$folder$") != null) {
            if (NativeS3FileSystem.LOG.isDebugEnabled()) {
                NativeS3FileSystem.LOG.debug("getFileStatus returning 'directory' for key '" + key + "' as '" + key + "_$folder$" + "' exists");
            }
            return this.newDirectory(absolutePath);
        }
        if (NativeS3FileSystem.LOG.isDebugEnabled()) {
            NativeS3FileSystem.LOG.debug("getFileStatus listing key '" + key + "'");
        }
        final PartialListing listing = this.store.list(key, 1);
        if (listing.getFiles().length > 0 || listing.getCommonPrefixes().length > 0) {
            if (NativeS3FileSystem.LOG.isDebugEnabled()) {
                NativeS3FileSystem.LOG.debug("getFileStatus returning 'directory' for key '" + key + "' as it has contents");
            }
            return this.newDirectory(absolutePath);
        }
        if (NativeS3FileSystem.LOG.isDebugEnabled()) {
            NativeS3FileSystem.LOG.debug("getFileStatus could not find key '" + key + "'");
        }
        throw new FileNotFoundException("No such file or directory '" + absolutePath + "'");
    }
    
    public URI getUri() {
        return this.uri;
    }
    
    public FileStatus[] listStatus(final Path f) throws IOException {
        final Path absolutePath = this.makeAbsolute(f);
        final String key = pathToKey(absolutePath);
        if (key.length() > 0) {
            final FileMetadata meta = this.store.retrieveMetadata(key);
            if (meta != null) {
                return new FileStatus[] { this.newFile(meta, absolutePath) };
            }
        }
        final URI pathUri = absolutePath.toUri();
        final Set<FileStatus> status = new TreeSet<FileStatus>();
        String priorLastKey = null;
        do {
            final PartialListing listing = this.store.list(key, 1000, priorLastKey, false);
            for (final FileMetadata fileMetadata : listing.getFiles()) {
                final Path subpath = keyToPath(fileMetadata.getKey());
                final String relativePath = pathUri.relativize(subpath.toUri()).getPath();
                if (!fileMetadata.getKey().equals(key + "/")) {
                    if (relativePath.endsWith("_$folder$")) {
                        status.add(this.newDirectory(new Path(absolutePath, relativePath.substring(0, relativePath.indexOf("_$folder$")))));
                    }
                    else {
                        status.add(this.newFile(fileMetadata, subpath));
                    }
                }
            }
            for (final String commonPrefix : listing.getCommonPrefixes()) {
                final Path subpath = keyToPath(commonPrefix);
                final String relativePath = pathUri.relativize(subpath.toUri()).getPath();
                if (!relativePath.isEmpty()) {
                    status.add(this.newDirectory(new Path(absolutePath, relativePath)));
                }
            }
            priorLastKey = listing.getPriorLastKey();
        } while (priorLastKey != null);
        if (status.isEmpty() && key.length() > 0 && this.store.retrieveMetadata(key + "_$folder$") == null) {
            throw new FileNotFoundException("File " + f + " does not exist.");
        }
        return status.toArray(new FileStatus[status.size()]);
    }
    
    private FileStatus newFile(final FileMetadata meta, final Path path) {
        return new FileStatus(meta.getLength(), false, 1, this.getDefaultBlockSize(), meta.getLastModified(), path.makeQualified(this.getUri(), this.getWorkingDirectory()));
    }
    
    private FileStatus newDirectory(final Path path) {
        return new FileStatus(0L, true, 1, 0L, 0L, path.makeQualified(this.getUri(), this.getWorkingDirectory()));
    }
    
    public boolean mkdirs(final Path f, final FsPermission permission) throws IOException {
        Path absolutePath = this.makeAbsolute(f);
        final List<Path> paths = new ArrayList<Path>();
        do {
            paths.add(0, absolutePath);
            absolutePath = absolutePath.getParent();
        } while (absolutePath != null);
        boolean result = true;
        for (final Path path : paths) {
            result &= this.mkdir(path);
        }
        return result;
    }
    
    private boolean mkdir(final Path f) throws IOException {
        try {
            final FileStatus fileStatus = this.getFileStatus(f);
            if (fileStatus.isFile()) {
                throw new FileAlreadyExistsException(String.format("Can't make directory for path '%s' since it is a file.", f));
            }
        }
        catch (FileNotFoundException e) {
            if (NativeS3FileSystem.LOG.isDebugEnabled()) {
                NativeS3FileSystem.LOG.debug("Making dir '" + f + "' in S3");
            }
            final String key = pathToKey(f) + "_$folder$";
            this.store.storeEmptyFile(key);
        }
        return true;
    }
    
    public FSDataInputStream open(final Path f, final int bufferSize) throws IOException {
        final FileStatus fs = this.getFileStatus(f);
        if (fs.isDirectory()) {
            throw new FileNotFoundException("'" + f + "' is a directory");
        }
        NativeS3FileSystem.LOG.info("Opening '" + f + "' for reading");
        final Path absolutePath = this.makeAbsolute(f);
        final String key = pathToKey(absolutePath);
        return new FSDataInputStream((InputStream)new BufferedFSInputStream((FSInputStream)new NativeS3FsInputStream(this.store, this.statistics, this.store.retrieve(key), key), bufferSize));
    }
    
    private void createParent(final Path path) throws IOException {
        final Path parent = path.getParent();
        if (parent != null) {
            final String key = pathToKey(this.makeAbsolute(parent));
            if (key.length() > 0) {
                this.store.storeEmptyFile(key + "_$folder$");
            }
        }
    }
    
    public boolean rename(final Path src, final Path dst) throws IOException {
        final String srcKey = pathToKey(this.makeAbsolute(src));
        if (srcKey.length() == 0) {
            return false;
        }
        final String debugPreamble = "Renaming '" + src + "' to '" + dst + "' - ";
        String dstKey;
        try {
            final boolean dstIsFile = this.getFileStatus(dst).isFile();
            if (dstIsFile) {
                if (NativeS3FileSystem.LOG.isDebugEnabled()) {
                    NativeS3FileSystem.LOG.debug(debugPreamble + "returning false as dst is an already existing file");
                }
                return false;
            }
            if (NativeS3FileSystem.LOG.isDebugEnabled()) {
                NativeS3FileSystem.LOG.debug(debugPreamble + "using dst as output directory");
            }
            dstKey = pathToKey(this.makeAbsolute(new Path(dst, src.getName())));
        }
        catch (FileNotFoundException e) {
            if (NativeS3FileSystem.LOG.isDebugEnabled()) {
                NativeS3FileSystem.LOG.debug(debugPreamble + "using dst as output destination");
            }
            dstKey = pathToKey(this.makeAbsolute(dst));
            try {
                if (this.getFileStatus(dst.getParent()).isFile()) {
                    if (NativeS3FileSystem.LOG.isDebugEnabled()) {
                        NativeS3FileSystem.LOG.debug(debugPreamble + "returning false as dst parent exists and is a file");
                    }
                    return false;
                }
            }
            catch (FileNotFoundException ex) {
                if (NativeS3FileSystem.LOG.isDebugEnabled()) {
                    NativeS3FileSystem.LOG.debug(debugPreamble + "returning false as dst parent does not exist");
                }
                return false;
            }
        }
        boolean srcIsFile;
        try {
            srcIsFile = this.getFileStatus(src).isFile();
        }
        catch (FileNotFoundException e2) {
            if (NativeS3FileSystem.LOG.isDebugEnabled()) {
                NativeS3FileSystem.LOG.debug(debugPreamble + "returning false as src does not exist");
            }
            return false;
        }
        if (srcIsFile) {
            if (NativeS3FileSystem.LOG.isDebugEnabled()) {
                NativeS3FileSystem.LOG.debug(debugPreamble + "src is file, so doing copy then delete in S3");
            }
            this.store.copy(srcKey, dstKey);
            this.store.delete(srcKey);
        }
        else {
            if (NativeS3FileSystem.LOG.isDebugEnabled()) {
                NativeS3FileSystem.LOG.debug(debugPreamble + "src is directory, so copying contents");
            }
            this.store.storeEmptyFile(dstKey + "_$folder$");
            final List<String> keysToDelete = new ArrayList<String>();
            String priorLastKey = null;
            do {
                final PartialListing listing = this.store.list(srcKey, 1000, priorLastKey, true);
                for (final FileMetadata file : listing.getFiles()) {
                    keysToDelete.add(file.getKey());
                    this.store.copy(file.getKey(), dstKey + file.getKey().substring(srcKey.length()));
                }
                priorLastKey = listing.getPriorLastKey();
            } while (priorLastKey != null);
            if (NativeS3FileSystem.LOG.isDebugEnabled()) {
                NativeS3FileSystem.LOG.debug(debugPreamble + "all files in src copied, now removing src files");
            }
            for (final String key : keysToDelete) {
                this.store.delete(key);
            }
            try {
                this.store.delete(srcKey + "_$folder$");
            }
            catch (FileNotFoundException ex2) {}
            if (NativeS3FileSystem.LOG.isDebugEnabled()) {
                NativeS3FileSystem.LOG.debug(debugPreamble + "done");
            }
        }
        return true;
    }
    
    public long getDefaultBlockSize() {
        return this.getConf().getLong("fs.s3n.block.size", 67108864L);
    }
    
    public void setWorkingDirectory(final Path newDir) {
        this.workingDir = newDir;
    }
    
    public Path getWorkingDirectory() {
        return this.workingDir;
    }
    
    public String getCanonicalServiceName() {
        return null;
    }
    
    static {
        LOG = LoggerFactory.getLogger((Class)NativeS3FileSystem.class);
    }
    
    static class NativeS3FsInputStream extends FSInputStream
    {
        private NativeFileSystemStore store;
        private FileSystem.Statistics statistics;
        private InputStream in;
        private final String key;
        private long pos;
        
        public NativeS3FsInputStream(final NativeFileSystemStore store, final FileSystem.Statistics statistics, final InputStream in, final String key) {
            this.pos = 0L;
            Preconditions.checkNotNull((Object)in, (Object)"Null input stream");
            this.store = store;
            this.statistics = statistics;
            this.in = in;
            this.key = key;
        }
        
        public synchronized int read() throws IOException {
            int result;
            try {
                result = this.in.read();
            }
            catch (IOException e) {
                NativeS3FileSystem.LOG.info("Received IOException while reading '{}', attempting to reopen", (Object)this.key);
                NativeS3FileSystem.LOG.debug("{}", (Object)e, (Object)e);
                try {
                    this.reopen(this.pos);
                    result = this.in.read();
                }
                catch (EOFException eof) {
                    NativeS3FileSystem.LOG.debug("EOF on input stream read: {}", (Object)eof, (Object)eof);
                    result = -1;
                }
            }
            if (result != -1) {
                ++this.pos;
            }
            if (this.statistics != null && result != -1) {
                this.statistics.incrementBytesRead(1L);
            }
            return result;
        }
        
        public synchronized int read(final byte[] b, final int off, final int len) throws IOException {
            if (this.in == null) {
                throw new EOFException("Cannot read closed stream");
            }
            int result = -1;
            try {
                result = this.in.read(b, off, len);
            }
            catch (EOFException eof) {
                throw eof;
            }
            catch (IOException e) {
                NativeS3FileSystem.LOG.info("Received IOException while reading '{}', attempting to reopen.", (Object)this.key);
                this.reopen(this.pos);
                result = this.in.read(b, off, len);
            }
            if (result > 0) {
                this.pos += result;
            }
            if (this.statistics != null && result > 0) {
                this.statistics.incrementBytesRead((long)result);
            }
            return result;
        }
        
        public synchronized void close() throws IOException {
            this.closeInnerStream();
        }
        
        private void closeInnerStream() {
            IOUtils.closeStream((Closeable)this.in);
            this.in = null;
        }
        
        private synchronized void reopen(final long pos) throws IOException {
            NativeS3FileSystem.LOG.debug("Reopening key '{}' for reading at position '{}", (Object)this.key, (Object)pos);
            final InputStream newStream = this.store.retrieve(this.key, pos);
            this.updateInnerStream(newStream, pos);
        }
        
        private synchronized void updateInnerStream(final InputStream newStream, final long newpos) throws IOException {
            Preconditions.checkNotNull((Object)newStream, (Object)"Null newstream argument");
            this.closeInnerStream();
            this.in = newStream;
            this.pos = newpos;
        }
        
        public synchronized void seek(final long newpos) throws IOException {
            if (newpos < 0L) {
                throw new EOFException("Cannot seek to a negative offset");
            }
            if (this.pos != newpos) {
                this.reopen(newpos);
            }
        }
        
        public synchronized long getPos() throws IOException {
            return this.pos;
        }
        
        public boolean seekToNewSource(final long targetPos) throws IOException {
            return false;
        }
    }
    
    private class NativeS3FsOutputStream extends OutputStream
    {
        private Configuration conf;
        private String key;
        private File backupFile;
        private OutputStream backupStream;
        private MessageDigest digest;
        private boolean closed;
        private LocalDirAllocator lDirAlloc;
        
        public NativeS3FsOutputStream(final Configuration conf, final NativeFileSystemStore store, final String key, final Progressable progress, final int bufferSize) throws IOException {
            this.conf = conf;
            this.key = key;
            this.backupFile = this.newBackupFile();
            NativeS3FileSystem.LOG.info("OutputStream for key '" + key + "' writing to tempfile '" + this.backupFile + "'");
            try {
                this.digest = MessageDigest.getInstance("MD5");
                this.backupStream = new BufferedOutputStream(new DigestOutputStream(new FileOutputStream(this.backupFile), this.digest));
            }
            catch (NoSuchAlgorithmException e) {
                NativeS3FileSystem.LOG.warn("Cannot load MD5 digest algorithm,skipping message integrity check.", (Throwable)e);
                this.backupStream = new BufferedOutputStream(new FileOutputStream(this.backupFile));
            }
        }
        
        private File newBackupFile() throws IOException {
            if (this.lDirAlloc == null) {
                this.lDirAlloc = new LocalDirAllocator("fs.s3.buffer.dir");
            }
            final File result = this.lDirAlloc.createTmpFileForWrite("output-", -1L, this.conf);
            result.deleteOnExit();
            return result;
        }
        
        @Override
        public void flush() throws IOException {
            this.backupStream.flush();
        }
        
        @Override
        public synchronized void close() throws IOException {
            if (this.closed) {
                return;
            }
            this.backupStream.close();
            NativeS3FileSystem.LOG.info("OutputStream for key '{}' closed. Now beginning upload", (Object)this.key);
            try {
                final byte[] md5Hash = (byte[])((this.digest == null) ? null : this.digest.digest());
                NativeS3FileSystem.this.store.storeFile(this.key, this.backupFile, md5Hash);
            }
            finally {
                if (!this.backupFile.delete()) {
                    NativeS3FileSystem.LOG.warn("Could not delete temporary s3n file: " + this.backupFile);
                }
                super.close();
                this.closed = true;
            }
            NativeS3FileSystem.LOG.info("OutputStream for key '{}' upload complete", (Object)this.key);
        }
        
        @Override
        public void write(final int b) throws IOException {
            this.backupStream.write(b);
        }
        
        @Override
        public void write(final byte[] b, final int off, final int len) throws IOException {
            this.backupStream.write(b, off, len);
        }
    }
}
