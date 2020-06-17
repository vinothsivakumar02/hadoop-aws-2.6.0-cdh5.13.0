// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3a;

import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.s3a.s3guard.MetadataStoreListFilesIterator;
import org.apache.hadoop.fs.PathFilter;
import java.util.Map;
import com.amazonaws.services.s3.model.SSEAwsKeyManagementParams;
import com.amazonaws.services.s3.model.SSECustomerKey;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.transfer.Copy;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import org.apache.hadoop.fs.LocalFileSystem;
import java.io.InterruptedIOException;
import com.amazonaws.event.ProgressListener;
import java.util.Collections;
import java.util.Set;
import org.apache.hadoop.fs.s3a.s3guard.DirListingMetadata;
import org.apache.hadoop.fs.PathIOException;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.transfer.Upload;
import com.google.common.base.Preconditions;
import com.amazonaws.services.s3.model.PutObjectRequest;
import java.util.Iterator;
import com.amazonaws.services.s3.model.MultiObjectDeleteException;
import org.apache.hadoop.fs.InvalidRequestException;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import org.apache.commons.lang.StringUtils;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import org.apache.hadoop.fs.RemoteIterator;
import java.util.List;
import org.apache.hadoop.fs.LocatedFileStatus;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import java.util.Collection;
import org.apache.hadoop.fs.s3a.s3guard.PathMetadata;
import java.util.ArrayList;
import java.util.HashSet;
import org.apache.hadoop.fs.CreateFlag;
import java.util.EnumSet;
import java.io.OutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.FileStatus;
import java.io.InputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import java.util.Objects;
import java.io.File;
import com.google.common.annotations.VisibleForTesting;
import java.util.Date;
import com.amazonaws.services.s3.transfer.TransferManagerConfiguration;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import java.io.FileNotFoundException;
import java.io.IOException;
import com.amazonaws.AmazonClientException;
import org.apache.hadoop.fs.s3a.s3guard.S3Guard;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.StorageStatistics;
import org.apache.hadoop.fs.GlobalStorageStatistics;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.fs.s3native.S3xLoginHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.s3guard.MetadataStore;
import java.util.concurrent.atomic.AtomicBoolean;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.slf4j.Logger;
import java.util.concurrent.ExecutorService;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.AmazonS3;
import org.apache.hadoop.fs.Path;
import java.net.URI;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.FileSystem;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class S3AFileSystem extends FileSystem
{
    public static final int DEFAULT_BLOCKSIZE = 33554432;
    private URI uri;
    private Path workingDir;
    private String username;
    private AmazonS3 s3;
    private String bucket;
    private int maxKeys;
    private Listing listing;
    private long partSize;
    private boolean enableMultiObjectsDelete;
    private TransferManager transfers;
    private ListeningExecutorService boundedThreadPool;
    private ExecutorService unboundedThreadPool;
    private long multiPartThreshold;
    public static final Logger LOG;
    private static final Logger PROGRESS;
    private LocalDirAllocator directoryAllocator;
    private CannedAccessControlList cannedACL;
    private S3AEncryptionMethods serverSideEncryptionAlgorithm;
    private S3AInstrumentation instrumentation;
    private S3AStorageStatistics storageStatistics;
    private long readAhead;
    private S3AInputPolicy inputPolicy;
    private static final AtomicBoolean warnedOfCoreThreadDeprecation;
    private final AtomicBoolean closed;
    private MetadataStore metadataStore;
    private boolean allowAuthoritative;
    private static final int MAX_ENTRIES_TO_DELETE = 1000;
    private boolean blockUploadEnabled;
    private String blockOutputBuffer;
    private S3ADataBlocks.BlockFactory blockFactory;
    private int blockOutputActiveBlocks;
    static final String DEPRECATED_ACCESS_KEY = "fs.s3a.awsAccessKeyId";
    static final String DEPRECATED_SECRET_KEY = "fs.s3a.awsSecretAccessKey";
    
    public S3AFileSystem() {
        this.closed = new AtomicBoolean(false);
    }
    
    private static void addDeprecatedKeys() {
        Configuration.addDeprecations(new Configuration.DeprecationDelta[] { new Configuration.DeprecationDelta("fs.s3a.server-side-encryption-key", "fs.s3a.server-side-encryption.key") });
        Configuration.reloadExistingConfigurations();
    }
    
    public void initialize(final URI name, final Configuration originalConf) throws IOException {
        this.uri = S3xLoginHelper.buildFSURI(name);
        this.bucket = name.getHost();
        final Configuration conf = S3AUtils.propagateBucketOptions(originalConf, this.bucket);
        S3AUtils.patchSecurityCredentialProviders(conf);
        super.initialize(name, conf);
        this.setConf(conf);
        try {
            this.instrumentation = new S3AInstrumentation(name);
            this.username = UserGroupInformation.getCurrentUser().getShortUserName();
            this.workingDir = new Path("/user", this.username).makeQualified(this.uri, this.getWorkingDirectory());
            final Class<? extends S3ClientFactory> s3ClientFactoryClass = (Class<? extends S3ClientFactory>)conf.getClass("fs.s3a.s3.client.factory.impl", (Class)Constants.DEFAULT_S3_CLIENT_FACTORY_IMPL, (Class)S3ClientFactory.class);
            this.s3 = ((S3ClientFactory)ReflectionUtils.newInstance((Class)s3ClientFactoryClass, conf)).createS3Client(name);
            this.maxKeys = S3AUtils.intOption(conf, "fs.s3a.paging.maximum", 5000, 1);
            this.listing = new Listing(this);
            this.partSize = S3AUtils.getMultipartSizeProperty(conf, "fs.s3a.multipart.size", 67108864L);
            this.multiPartThreshold = S3AUtils.getMultipartSizeProperty(conf, "fs.s3a.multipart.threshold", 134217728L);
            S3AUtils.longBytesOption(conf, "fs.s3a.block.size", 33554432L, 1L);
            this.enableMultiObjectsDelete = conf.getBoolean("fs.s3a.multiobjectdelete.enable", true);
            this.readAhead = S3AUtils.longBytesOption(conf, "fs.s3a.readahead.range", 65536L, 0L);
            this.storageStatistics = (S3AStorageStatistics)GlobalStorageStatistics.INSTANCE.put("S3AStorageStatistics", (GlobalStorageStatistics.StorageStatisticsProvider)new GlobalStorageStatistics.StorageStatisticsProvider() {
                public StorageStatistics provide() {
                    return new S3AStorageStatistics();
                }
            });
            if (conf.get("fs.s3a.threads.core") != null && S3AFileSystem.warnedOfCoreThreadDeprecation.compareAndSet(false, true)) {
                LoggerFactory.getLogger("org.apache.hadoop.conf.Configuration.deprecation").warn("Unsupported option \"fs.s3a.threads.core\" will be ignored {}", (Object)conf.get("fs.s3a.threads.core"));
            }
            int maxThreads = conf.getInt("fs.s3a.threads.max", 10);
            if (maxThreads < 2) {
                S3AFileSystem.LOG.warn("fs.s3a.threads.max must be at least 2: forcing to 2.");
                maxThreads = 2;
            }
            final int totalTasks = S3AUtils.intOption(conf, "fs.s3a.max.total.tasks", 5, 1);
            final long keepAliveTime = S3AUtils.longOption(conf, "fs.s3a.threads.keepalivetime", 60L, 0L);
            this.boundedThreadPool = (ListeningExecutorService)BlockingThreadPoolExecutorService.newInstance(maxThreads, maxThreads + totalTasks, keepAliveTime, TimeUnit.SECONDS, "s3a-transfer-shared");
            this.unboundedThreadPool = new ThreadPoolExecutor(maxThreads, Integer.MAX_VALUE, keepAliveTime, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(), BlockingThreadPoolExecutorService.newDaemonThreadFactory("s3a-transfer-unbounded"));
            this.initTransferManager();
            this.initCannedAcls(conf);
            this.verifyBucketExists();
            this.initMultipartUploads(conf);
            this.serverSideEncryptionAlgorithm = S3AUtils.getEncryptionAlgorithm(conf);
            this.inputPolicy = S3AInputPolicy.getPolicy(conf.getTrimmed("fs.s3a.experimental.input.fadvise", "normal"));
            this.blockUploadEnabled = conf.getBoolean("fs.s3a.fast.upload", false);
            if (this.blockUploadEnabled) {
                this.blockOutputBuffer = conf.getTrimmed("fs.s3a.fast.upload.buffer", "disk");
                this.partSize = S3AUtils.ensureOutputParameterInRange("fs.s3a.multipart.size", this.partSize);
                this.blockFactory = S3ADataBlocks.createFactory(this, this.blockOutputBuffer);
                this.blockOutputActiveBlocks = S3AUtils.intOption(conf, "fs.s3a.fast.upload.active.blocks", 4, 1);
                S3AFileSystem.LOG.debug("Using S3ABlockOutputStream with buffer = {}; block={}; queue limit={}", new Object[] { this.blockOutputBuffer, this.partSize, this.blockOutputActiveBlocks });
            }
            else {
                S3AFileSystem.LOG.debug("Using S3AOutputStream");
            }
            this.metadataStore = S3Guard.getMetadataStore(this);
            this.allowAuthoritative = conf.getBoolean("fs.s3a.metadatastore.authoritative", false);
        }
        catch (AmazonClientException e) {
            throw S3AUtils.translateException("initializing ", new Path(name), e);
        }
    }
    
    protected void verifyBucketExists() throws FileNotFoundException, IOException {
        try {
            if (!this.s3.doesBucketExist(this.bucket)) {
                throw new FileNotFoundException("Bucket " + this.bucket + " does not exist");
            }
        }
        catch (AmazonS3Exception e) {
            S3AFileSystem.LOG.warn(S3AUtils.stringify(e), (Throwable)e);
            throw S3AUtils.translateException("doesBucketExist", this.bucket, (AmazonClientException)e);
        }
        catch (AmazonServiceException e2) {
            S3AFileSystem.LOG.warn(S3AUtils.stringify(e2), (Throwable)e2);
            throw S3AUtils.translateException("doesBucketExist", this.bucket, (AmazonClientException)e2);
        }
        catch (AmazonClientException e3) {
            throw S3AUtils.translateException("doesBucketExist", this.bucket, e3);
        }
    }
    
    public S3AInstrumentation getInstrumentation() {
        return this.instrumentation;
    }
    
    private void initTransferManager() {
        final TransferManagerConfiguration transferConfiguration = new TransferManagerConfiguration();
        transferConfiguration.setMinimumUploadPartSize(this.partSize);
        transferConfiguration.setMultipartUploadThreshold(this.multiPartThreshold);
        transferConfiguration.setMultipartCopyPartSize(this.partSize);
        transferConfiguration.setMultipartCopyThreshold(this.multiPartThreshold);
        (this.transfers = new TransferManager(this.s3, this.unboundedThreadPool)).setConfiguration(transferConfiguration);
    }
    
    private void initCannedAcls(final Configuration conf) {
        final String cannedACLName = conf.get("fs.s3a.acl.default", "");
        if (!cannedACLName.isEmpty()) {
            this.cannedACL = CannedAccessControlList.valueOf(cannedACLName);
        }
        else {
            this.cannedACL = null;
        }
    }
    
    private void initMultipartUploads(final Configuration conf) throws IOException {
        final boolean purgeExistingMultipart = conf.getBoolean("fs.s3a.multipart.purge", false);
        final long purgeExistingMultipartAge = S3AUtils.longOption(conf, "fs.s3a.multipart.purge.age", 86400L, 0L);
        if (purgeExistingMultipart) {
            final Date purgeBefore = new Date(new Date().getTime() - purgeExistingMultipartAge * 1000L);
            try {
                this.transfers.abortMultipartUploads(this.bucket, purgeBefore);
            }
            catch (AmazonServiceException e) {
                if (e.getStatusCode() != 403) {
                    throw S3AUtils.translateException("purging multipart uploads", this.bucket, (AmazonClientException)e);
                }
                this.instrumentation.errorIgnored();
                S3AFileSystem.LOG.debug("Failed to purging multipart uploads against {}, FS may be read only", (Object)this.bucket, (Object)e);
            }
        }
    }
    
    public String getScheme() {
        return "s3a";
    }
    
    public URI getUri() {
        return this.uri;
    }
    
    public int getDefaultPort() {
        return -1;
    }
    
    AmazonS3 getAmazonS3Client() {
        return this.s3;
    }
    
    public String getBucketLocation() throws IOException {
        return this.getBucketLocation(this.bucket);
    }
    
    public String getBucketLocation(final String bucketName) throws IOException {
        try {
            return this.s3.getBucketLocation(bucketName);
        }
        catch (AmazonClientException e) {
            throw S3AUtils.translateException("getBucketLocation()", bucketName, e);
        }
    }
    
    @VisibleForTesting
    long getReadAheadRange() {
        return this.readAhead;
    }
    
    @InterfaceStability.Unstable
    public S3AInputPolicy getInputPolicy() {
        return this.inputPolicy;
    }
    
    synchronized File createTmpFileForWrite(final String pathStr, final long size, final Configuration conf) throws IOException {
        if (this.directoryAllocator == null) {
            final String bufferDir = (conf.get("fs.s3a.buffer.dir") != null) ? "fs.s3a.buffer.dir" : "hadoop.tmp.dir";
            this.directoryAllocator = new LocalDirAllocator(bufferDir);
        }
        return this.directoryAllocator.createTmpFileForWrite(pathStr, size, conf);
    }
    
    public String getBucket() {
        return this.bucket;
    }
    
    @InterfaceStability.Unstable
    public void setInputPolicy(final S3AInputPolicy inputPolicy) {
        Objects.requireNonNull(inputPolicy, "Null inputStrategy");
        S3AFileSystem.LOG.debug("Setting input strategy: {}", (Object)inputPolicy);
        this.inputPolicy = inputPolicy;
    }
    
    @VisibleForTesting
    String pathToKey(Path path) {
        if (!path.isAbsolute()) {
            path = new Path(this.workingDir, path);
        }
        if (path.toUri().getScheme() != null && path.toUri().getPath().isEmpty()) {
            return "";
        }
        return path.toUri().getPath().substring(1);
    }
    
    private String maybeAddTrailingSlash(final String key) {
        if (!key.isEmpty() && !key.endsWith("/")) {
            return key + '/';
        }
        return key;
    }
    
    private Path keyToPath(final String key) {
        return new Path("/" + key);
    }
    
    Path keyToQualifiedPath(final String key) {
        return this.qualify(this.keyToPath(key));
    }
    
    public Path qualify(final Path path) {
        return path.makeQualified(this.uri, this.workingDir);
    }
    
    public void checkPath(final Path path) {
        S3xLoginHelper.checkPath(this.getConf(), this.getUri(), path, this.getDefaultPort());
    }
    
    protected URI canonicalizeUri(final URI rawUri) {
        return S3xLoginHelper.canonicalizeUri(rawUri, this.getDefaultPort());
    }
    
    public FSDataInputStream open(final Path f, final int bufferSize) throws IOException {
        S3AFileSystem.LOG.debug("Opening '{}' for reading.", (Object)f);
        final FileStatus fileStatus = this.getFileStatus(f);
        if (fileStatus.isDirectory()) {
            throw new FileNotFoundException("Can't open " + f + " because it is a directory");
        }
        return new FSDataInputStream((InputStream)new S3AInputStream(new S3ObjectAttributes(this.bucket, this.pathToKey(f), this.serverSideEncryptionAlgorithm, S3AUtils.getServerSideEncryptionKey(this.getConf())), fileStatus.getLen(), this.s3, this.statistics, this.instrumentation, this.readAhead, this.inputPolicy));
    }
    
    public FSDataOutputStream create(final Path f, final FsPermission permission, final boolean overwrite, final int bufferSize, final short replication, final long blockSize, final Progressable progress) throws IOException {
        final String key = this.pathToKey(f);
        FileStatus status = null;
        try {
            status = this.getFileStatus(f);
            if (status.isDirectory()) {
                throw new FileAlreadyExistsException(f + " is a directory");
            }
            if (!overwrite) {
                throw new FileAlreadyExistsException(f + " already exists");
            }
            S3AFileSystem.LOG.debug("Overwriting file {}", (Object)f);
        }
        catch (FileNotFoundException ex) {}
        this.instrumentation.fileCreated();
        FSDataOutputStream output;
        if (this.blockUploadEnabled) {
            output = new FSDataOutputStream((OutputStream)new S3ABlockOutputStream(this, key, (ExecutorService)new SemaphoredDelegatingExecutor(this.boundedThreadPool, this.blockOutputActiveBlocks, true), progress, this.partSize, this.blockFactory, this.instrumentation.newOutputStreamStatistics(this.statistics), new WriteOperationHelper(key)), (FileSystem.Statistics)null);
        }
        else {
            output = new FSDataOutputStream((OutputStream)new S3AOutputStream(this.getConf(), this, key, progress), (FileSystem.Statistics)null);
        }
        return output;
    }
    
    public FSDataOutputStream createNonRecursive(final Path path, final FsPermission permission, final EnumSet<CreateFlag> flags, final int bufferSize, final short replication, final long blockSize, final Progressable progress) throws IOException {
        final Path parent = path.getParent();
        if (parent != null && !this.getFileStatus(parent).isDirectory()) {
            throw new FileAlreadyExistsException("Not a directory: " + parent);
        }
        return this.create(path, permission, flags.contains(CreateFlag.OVERWRITE), bufferSize, replication, blockSize, progress);
    }
    
    public FSDataOutputStream append(final Path f, final int bufferSize, final Progressable progress) throws IOException {
        throw new IOException("Not supported");
    }
    
    public boolean rename(final Path src, final Path dst) throws IOException {
        try {
            return this.innerRename(src, dst);
        }
        catch (AmazonClientException e) {
            throw S3AUtils.translateException("rename(" + src + ", " + dst + ")", src, e);
        }
        catch (RenameFailedException e2) {
            S3AFileSystem.LOG.debug(e2.getMessage());
            return e2.getExitCode();
        }
        catch (FileNotFoundException e3) {
            S3AFileSystem.LOG.debug(e3.toString());
            return false;
        }
    }
    
    private boolean innerRename(final Path source, final Path dest) throws RenameFailedException, FileNotFoundException, IOException, AmazonClientException {
        final Path src = this.qualify(source);
        final Path dst = this.qualify(dest);
        S3AFileSystem.LOG.debug("Rename path {} to {}", (Object)src, (Object)dst);
        this.incrementStatistic(Statistic.INVOCATION_RENAME);
        String srcKey = this.pathToKey(src);
        String dstKey = this.pathToKey(dst);
        if (srcKey.isEmpty()) {
            throw new RenameFailedException(src, dst, "source is root directory");
        }
        if (dstKey.isEmpty()) {
            throw new RenameFailedException(src, dst, "dest is root directory");
        }
        final S3AFileStatus srcStatus = this.innerGetFileStatus(src, true);
        if (srcKey.equals(dstKey)) {
            S3AFileSystem.LOG.debug("rename: src and dest refer to the same file or directory: {}", (Object)dst);
            throw new RenameFailedException(src, dst, "source and dest refer to the same file or directory").withExitCode(srcStatus.isFile());
        }
        S3AFileStatus dstStatus = null;
        try {
            dstStatus = this.innerGetFileStatus(dst, true);
            if (srcStatus.isDirectory()) {
                if (dstStatus.isFile()) {
                    throw new RenameFailedException(src, dst, "source is a directory and dest is a file").withExitCode(srcStatus.isFile());
                }
                if (dstStatus.isEmptyDirectory() != Tristate.TRUE) {
                    throw new RenameFailedException(src, dst, "Destination is a non-empty directory").withExitCode(false);
                }
            }
            else if (dstStatus.isFile()) {
                throw new RenameFailedException(src, dst, "Cannot rename onto an existing file").withExitCode(false);
            }
        }
        catch (FileNotFoundException e) {
            S3AFileSystem.LOG.debug("rename: destination path {} not found", (Object)dst);
            final Path parent = dst.getParent();
            if (!this.pathToKey(parent).isEmpty()) {
                try {
                    final S3AFileStatus dstParentStatus = this.innerGetFileStatus(dst.getParent(), false);
                    if (!dstParentStatus.isDirectory()) {
                        throw new RenameFailedException(src, dst, "destination parent is not a directory");
                    }
                }
                catch (FileNotFoundException e2) {
                    throw new RenameFailedException(src, dst, "destination has no parent ");
                }
            }
        }
        Collection<Path> srcPaths = null;
        List<PathMetadata> dstMetas = null;
        if (this.hasMetadataStore()) {
            srcPaths = new HashSet<Path>();
            dstMetas = new ArrayList<PathMetadata>();
        }
        if (srcStatus.isFile()) {
            S3AFileSystem.LOG.debug("rename: renaming file {} to {}", (Object)src, (Object)dst);
            final long length = srcStatus.getLen();
            if (dstStatus != null && dstStatus.isDirectory()) {
                String newDstKey = dstKey;
                if (!newDstKey.endsWith("/")) {
                    newDstKey += "/";
                }
                final String filename = srcKey.substring(this.pathToKey(src.getParent()).length() + 1);
                newDstKey += filename;
                this.copyFile(srcKey, newDstKey, length);
                S3Guard.addMoveFile(this.metadataStore, srcPaths, dstMetas, src, this.keyToQualifiedPath(newDstKey), length, this.getDefaultBlockSize(dst), this.username);
            }
            else {
                this.copyFile(srcKey, dstKey, srcStatus.getLen());
                S3Guard.addMoveFile(this.metadataStore, srcPaths, dstMetas, src, dst, length, this.getDefaultBlockSize(dst), this.username);
            }
            this.innerDelete(srcStatus, false);
        }
        else {
            S3AFileSystem.LOG.debug("rename: renaming directory {} to {}", (Object)src, (Object)dst);
            if (!dstKey.endsWith("/")) {
                dstKey += "/";
            }
            if (!srcKey.endsWith("/")) {
                srcKey += "/";
            }
            if (dstKey.startsWith(srcKey)) {
                throw new RenameFailedException(srcKey, dstKey, "cannot rename a directory to a subdirectory of itself ");
            }
            final List<DeleteObjectsRequest.KeyVersion> keysToDelete = new ArrayList<DeleteObjectsRequest.KeyVersion>();
            if (dstStatus != null && dstStatus.isEmptyDirectory() == Tristate.TRUE) {
                keysToDelete.add(new DeleteObjectsRequest.KeyVersion(dstKey));
            }
            final Path parentPath = this.keyToPath(srcKey);
            final RemoteIterator<LocatedFileStatus> iterator = this.listFilesAndEmptyDirectories(parentPath, true);
            while (iterator.hasNext()) {
                final LocatedFileStatus status = (LocatedFileStatus)iterator.next();
                final long length2 = status.getLen();
                String key = this.pathToKey(status.getPath());
                if (status.isDirectory() && !key.endsWith("/")) {
                    key += "/";
                }
                keysToDelete.add(new DeleteObjectsRequest.KeyVersion(key));
                final String newDstKey2 = dstKey + key.substring(srcKey.length());
                this.copyFile(key, newDstKey2, length2);
                if (this.hasMetadataStore()) {
                    final Path childSrc = this.keyToQualifiedPath(key);
                    final Path childDst = this.keyToQualifiedPath(newDstKey2);
                    if (S3AUtils.objectRepresentsDirectory(key, length2)) {
                        S3Guard.addMoveDir(this.metadataStore, srcPaths, dstMetas, childSrc, childDst, this.username);
                    }
                    else {
                        S3Guard.addMoveFile(this.metadataStore, srcPaths, dstMetas, childSrc, childDst, length2, this.getDefaultBlockSize(childDst), this.username);
                    }
                    S3Guard.addMoveAncestors(this.metadataStore, srcPaths, dstMetas, this.keyToQualifiedPath(srcKey), childSrc, childDst, this.username);
                }
                if (keysToDelete.size() == 1000) {
                    this.removeKeys(keysToDelete, true, false);
                }
            }
            if (!keysToDelete.isEmpty()) {
                this.removeKeys(keysToDelete, false, false);
            }
            if (this.hasMetadataStore() && srcPaths != null && !srcPaths.contains(src)) {
                S3AFileSystem.LOG.debug("To move the non-empty top-level dir src={} and dst={}", (Object)src, (Object)dst);
                S3Guard.addMoveDir(this.metadataStore, srcPaths, dstMetas, src, dst, this.username);
            }
        }
        this.metadataStore.move(srcPaths, dstMetas);
        if (src.getParent() != dst.getParent()) {
            this.deleteUnnecessaryFakeDirectories(dst.getParent());
            this.createFakeDirectoryIfNecessary(src.getParent());
        }
        return true;
    }
    
    @VisibleForTesting
    public ObjectMetadata getObjectMetadata(final Path path) throws IOException {
        return this.getObjectMetadata(this.pathToKey(path));
    }
    
    public boolean hasMetadataStore() {
        return !S3Guard.isNullMetadataStore(this.metadataStore);
    }
    
    @VisibleForTesting
    MetadataStore getMetadataStore() {
        return this.metadataStore;
    }
    
    @VisibleForTesting
    void setMetadataStore(final MetadataStore ms) {
        this.metadataStore = ms;
    }
    
    protected void incrementStatistic(final Statistic statistic) {
        this.incrementStatistic(statistic, 1L);
    }
    
    protected void incrementStatistic(final Statistic statistic, final long count) {
        this.instrumentation.incrementCounter(statistic, count);
        this.storageStatistics.incrementCounter(statistic, count);
    }
    
    protected void decrementGauge(final Statistic statistic, final long count) {
        this.instrumentation.decrementGauge(statistic, count);
    }
    
    protected void incrementGauge(final Statistic statistic, final long count) {
        this.instrumentation.incrementGauge(statistic, count);
    }
    
    public S3AStorageStatistics getStorageStatistics() {
        return this.storageStatistics;
    }
    
    protected ObjectMetadata getObjectMetadata(final String key) {
        this.incrementStatistic(Statistic.OBJECT_METADATA_REQUESTS);
        final GetObjectMetadataRequest request = new GetObjectMetadataRequest(this.bucket, key);
        if (S3AEncryptionMethods.SSE_C.equals(this.serverSideEncryptionAlgorithm) && StringUtils.isNotBlank(S3AUtils.getServerSideEncryptionKey(this.getConf()))) {
            request.setSSECustomerKey(this.generateSSECustomerKey());
        }
        final ObjectMetadata meta = this.s3.getObjectMetadata(request);
        this.incrementReadOperations();
        return meta;
    }
    
    protected ObjectListing listObjects(final ListObjectsRequest request) {
        this.incrementStatistic(Statistic.OBJECT_LIST_REQUESTS);
        this.incrementReadOperations();
        return this.s3.listObjects(request);
    }
    
    protected ObjectListing continueListObjects(final ObjectListing objects) {
        this.incrementStatistic(Statistic.OBJECT_CONTINUE_LIST_REQUESTS);
        this.incrementReadOperations();
        return this.s3.listNextBatchOfObjects(objects);
    }
    
    public void incrementReadOperations() {
        this.statistics.incrementReadOps(1);
    }
    
    public void incrementWriteOperations() {
        this.statistics.incrementWriteOps(1);
    }
    
    private void deleteObject(final String key) throws InvalidRequestException {
        this.blockRootDelete(key);
        this.incrementWriteOperations();
        this.incrementStatistic(Statistic.OBJECT_DELETE_REQUESTS);
        this.s3.deleteObject(this.bucket, key);
    }
    
    private void blockRootDelete(final String key) throws InvalidRequestException {
        if (key.isEmpty() || "/".equals(key)) {
            throw new InvalidRequestException("Bucket " + this.bucket + " cannot be deleted");
        }
    }
    
    private void deleteObjects(final DeleteObjectsRequest deleteRequest) throws MultiObjectDeleteException, AmazonClientException {
        this.incrementWriteOperations();
        this.incrementStatistic(Statistic.OBJECT_DELETE_REQUESTS, 1L);
        try {
            this.s3.deleteObjects(deleteRequest);
        }
        catch (MultiObjectDeleteException e) {
            final List<MultiObjectDeleteException.DeleteError> errors = (List<MultiObjectDeleteException.DeleteError>)e.getErrors();
            S3AFileSystem.LOG.error("Partial failure of delete, {} errors", (Object)errors.size(), (Object)e);
            for (final MultiObjectDeleteException.DeleteError error : errors) {
                S3AFileSystem.LOG.error("{}: \"{}\" - {}", new Object[] { error.getKey(), error.getCode(), error.getMessage() });
            }
            throw e;
        }
    }
    
    public PutObjectRequest newPutObjectRequest(final String key, final ObjectMetadata metadata, final File srcfile) {
        Preconditions.checkNotNull((Object)srcfile);
        final PutObjectRequest putObjectRequest = new PutObjectRequest(this.bucket, key, srcfile);
        this.setOptionalPutRequestParameters(putObjectRequest);
        putObjectRequest.setCannedAcl(this.cannedACL);
        putObjectRequest.setMetadata(metadata);
        return putObjectRequest;
    }
    
    private PutObjectRequest newPutObjectRequest(final String key, final ObjectMetadata metadata, final InputStream inputStream) {
        Preconditions.checkNotNull((Object)inputStream);
        final PutObjectRequest putObjectRequest = new PutObjectRequest(this.bucket, key, inputStream, metadata);
        this.setOptionalPutRequestParameters(putObjectRequest);
        putObjectRequest.setCannedAcl(this.cannedACL);
        return putObjectRequest;
    }
    
    public ObjectMetadata newObjectMetadata() {
        final ObjectMetadata om = new ObjectMetadata();
        this.setOptionalObjectMetadata(om);
        return om;
    }
    
    public ObjectMetadata newObjectMetadata(final long length) {
        final ObjectMetadata om = this.newObjectMetadata();
        if (length >= 0L) {
            om.setContentLength(length);
        }
        return om;
    }
    
    public UploadInfo putObject(final PutObjectRequest putObjectRequest) {
        long len;
        if (putObjectRequest.getFile() != null) {
            len = putObjectRequest.getFile().length();
        }
        else {
            len = putObjectRequest.getMetadata().getContentLength();
        }
        this.incrementPutStartStatistics(len);
        try {
            final Upload upload = this.transfers.upload(putObjectRequest);
            this.incrementPutCompletedStatistics(true, len);
            return new UploadInfo(upload, len);
        }
        catch (AmazonClientException e) {
            this.incrementPutCompletedStatistics(false, len);
            throw e;
        }
    }
    
    public PutObjectResult putObjectDirect(final PutObjectRequest putObjectRequest) throws AmazonClientException {
        long len;
        if (putObjectRequest.getFile() != null) {
            len = putObjectRequest.getFile().length();
        }
        else {
            len = putObjectRequest.getMetadata().getContentLength();
        }
        this.incrementPutStartStatistics(len);
        try {
            final PutObjectResult result = this.s3.putObject(putObjectRequest);
            this.incrementPutCompletedStatistics(true, len);
            return result;
        }
        catch (AmazonClientException e) {
            this.incrementPutCompletedStatistics(false, len);
            throw e;
        }
    }
    
    public UploadPartResult uploadPart(final UploadPartRequest request) throws AmazonClientException {
        final long len = request.getPartSize();
        this.incrementPutStartStatistics(len);
        try {
            final UploadPartResult uploadPartResult = this.s3.uploadPart(request);
            this.incrementPutCompletedStatistics(true, len);
            return uploadPartResult;
        }
        catch (AmazonClientException e) {
            this.incrementPutCompletedStatistics(false, len);
            throw e;
        }
    }
    
    public void incrementPutStartStatistics(final long bytes) {
        S3AFileSystem.LOG.debug("PUT start {} bytes", (Object)bytes);
        this.incrementWriteOperations();
        this.incrementStatistic(Statistic.OBJECT_PUT_REQUESTS);
        this.incrementGauge(Statistic.OBJECT_PUT_REQUESTS_ACTIVE, 1L);
        if (bytes > 0L) {
            this.incrementGauge(Statistic.OBJECT_PUT_BYTES_PENDING, bytes);
        }
    }
    
    public void incrementPutCompletedStatistics(final boolean success, final long bytes) {
        S3AFileSystem.LOG.debug("PUT completed success={}; {} bytes", (Object)success, (Object)bytes);
        this.incrementWriteOperations();
        if (bytes > 0L) {
            this.incrementStatistic(Statistic.OBJECT_PUT_BYTES, bytes);
            this.decrementGauge(Statistic.OBJECT_PUT_BYTES_PENDING, bytes);
        }
        this.incrementStatistic(Statistic.OBJECT_PUT_REQUESTS_COMPLETED);
        this.decrementGauge(Statistic.OBJECT_PUT_REQUESTS_ACTIVE, 1L);
    }
    
    public void incrementPutProgressStatistics(final String key, final long bytes) {
        S3AFileSystem.PROGRESS.debug("PUT {}: {} bytes", (Object)key, (Object)bytes);
        this.incrementWriteOperations();
        if (bytes > 0L) {
            this.statistics.incrementBytesWritten(bytes);
        }
    }
    
    @VisibleForTesting
    void removeKeys(final List<DeleteObjectsRequest.KeyVersion> keysToDelete, final boolean clearKeys, final boolean deleteFakeDir) throws MultiObjectDeleteException, AmazonClientException, InvalidRequestException {
        if (keysToDelete.isEmpty()) {
            return;
        }
        for (final DeleteObjectsRequest.KeyVersion keyVersion : keysToDelete) {
            this.blockRootDelete(keyVersion.getKey());
        }
        if (this.enableMultiObjectsDelete) {
            this.deleteObjects(new DeleteObjectsRequest(this.bucket).withKeys((List)keysToDelete));
        }
        else {
            for (final DeleteObjectsRequest.KeyVersion keyVersion : keysToDelete) {
                this.deleteObject(keyVersion.getKey());
            }
        }
        if (!deleteFakeDir) {
            this.instrumentation.fileDeleted(keysToDelete.size());
        }
        else {
            this.instrumentation.fakeDirsDeleted(keysToDelete.size());
        }
        if (clearKeys) {
            keysToDelete.clear();
        }
    }
    
    public boolean delete(final Path f, final boolean recursive) throws IOException {
        try {
            return this.innerDelete(this.innerGetFileStatus(f, true), recursive);
        }
        catch (FileNotFoundException e2) {
            S3AFileSystem.LOG.debug("Couldn't delete {} - does not exist", (Object)f);
            this.instrumentation.errorIgnored();
            return false;
        }
        catch (AmazonClientException e) {
            throw S3AUtils.translateException("delete", f, e);
        }
    }
    
    private boolean innerDelete(final S3AFileStatus status, final boolean recursive) throws IOException, AmazonClientException {
        final Path f = status.getPath();
        S3AFileSystem.LOG.debug("Delete path {} - recursive {}", (Object)f, (Object)recursive);
        String key = this.pathToKey(f);
        if (status.isDirectory()) {
            S3AFileSystem.LOG.debug("delete: Path is a directory: {}", (Object)f);
            Preconditions.checkArgument(status.isEmptyDirectory() != Tristate.UNKNOWN, (Object)"File status must have directory emptiness computed");
            if (!key.endsWith("/")) {
                key += "/";
            }
            if (key.equals("/")) {
                return this.rejectRootDirectoryDelete(status, recursive);
            }
            if (!recursive && status.isEmptyDirectory() == Tristate.FALSE) {
                throw new PathIsNotEmptyDirectoryException(f.toString());
            }
            if (status.isEmptyDirectory() == Tristate.TRUE) {
                S3AFileSystem.LOG.debug("Deleting fake empty directory {}", (Object)key);
                this.deleteObject(key);
                this.metadataStore.delete(f);
                this.instrumentation.directoryDeleted();
            }
            else {
                S3AFileSystem.LOG.debug("Getting objects for directory prefix {} to delete", (Object)key);
                final ListObjectsRequest request = this.createListObjectsRequest(key, null);
                ObjectListing objects = this.listObjects(request);
                final List<DeleteObjectsRequest.KeyVersion> keys = new ArrayList<DeleteObjectsRequest.KeyVersion>(objects.getObjectSummaries().size());
                while (true) {
                    for (final S3ObjectSummary summary : objects.getObjectSummaries()) {
                        keys.add(new DeleteObjectsRequest.KeyVersion(summary.getKey()));
                        S3AFileSystem.LOG.debug("Got object to delete {}", (Object)summary.getKey());
                        if (keys.size() == 1000) {
                            this.removeKeys(keys, true, false);
                        }
                    }
                    if (!objects.isTruncated()) {
                        break;
                    }
                    objects = this.continueListObjects(objects);
                }
                if (!keys.isEmpty()) {
                    this.removeKeys(keys, false, false);
                }
            }
            this.metadataStore.deleteSubtree(f);
        }
        else {
            S3AFileSystem.LOG.debug("delete: Path is a file");
            this.instrumentation.fileDeleted(1);
            this.deleteObject(key);
            this.metadataStore.delete(f);
        }
        final Path parent = f.getParent();
        if (parent != null) {
            this.createFakeDirectoryIfNecessary(parent);
        }
        return true;
    }
    
    private boolean rejectRootDirectoryDelete(final S3AFileStatus status, final boolean recursive) throws IOException {
        S3AFileSystem.LOG.info("s3a delete the {} root directory of {}", (Object)this.bucket, (Object)recursive);
        final boolean emptyRoot = status.isEmptyDirectory() == Tristate.TRUE;
        if (emptyRoot) {
            return true;
        }
        if (recursive) {
            return false;
        }
        throw new PathIOException(this.bucket, "Cannot delete root path");
    }
    
    private void createFakeDirectoryIfNecessary(final Path f) throws IOException, AmazonClientException {
        final String key = this.pathToKey(f);
        if (!key.isEmpty() && !this.s3Exists(f)) {
            S3AFileSystem.LOG.debug("Creating new fake directory at {}", (Object)f);
            this.createFakeDirectory(key);
        }
    }
    
    public FileStatus[] listStatus(final Path f) throws FileNotFoundException, IOException {
        try {
            return this.innerListStatus(f);
        }
        catch (AmazonClientException e) {
            throw S3AUtils.translateException("listStatus", f, e);
        }
    }
    
    public FileStatus[] innerListStatus(final Path f) throws FileNotFoundException, IOException, AmazonClientException {
        final Path path = this.qualify(f);
        String key = this.pathToKey(path);
        S3AFileSystem.LOG.debug("List status for path: {}", (Object)path);
        this.incrementStatistic(Statistic.INVOCATION_LIST_STATUS);
        final FileStatus fileStatus = this.getFileStatus(path);
        if (!fileStatus.isDirectory()) {
            S3AFileSystem.LOG.debug("Adding: rd (not a dir): {}", (Object)path);
            final FileStatus[] stats = { fileStatus };
            return stats;
        }
        if (!key.isEmpty()) {
            key += '/';
        }
        final DirListingMetadata dirMeta = this.metadataStore.listChildren(path);
        if (this.allowAuthoritative && dirMeta != null && dirMeta.isAuthoritative()) {
            return S3Guard.dirMetaToStatuses(dirMeta);
        }
        final ListObjectsRequest request = this.createListObjectsRequest(key, "/");
        S3AFileSystem.LOG.debug("listStatus: doing listObjects for directory {}", (Object)key);
        final Listing.FileStatusListingIterator files = this.listing.createFileStatusListingIterator(path, request, Listing.ACCEPT_ALL, new Listing.AcceptAllButSelfAndS3nDirs(path));
        final List<FileStatus> result = new ArrayList<FileStatus>(files.getBatchSize());
        while (files.hasNext()) {
            result.add(files.next());
        }
        return S3Guard.dirListingUnion(this.metadataStore, path, result, dirMeta, this.allowAuthoritative);
    }
    
    private ListObjectsRequest createListObjectsRequest(final String key, final String delimiter) {
        final ListObjectsRequest request = new ListObjectsRequest();
        request.setBucketName(this.bucket);
        request.setMaxKeys(Integer.valueOf(this.maxKeys));
        request.setPrefix(key);
        if (delimiter != null) {
            request.setDelimiter(delimiter);
        }
        return request;
    }
    
    public void setWorkingDirectory(final Path newDir) {
        this.workingDir = newDir;
    }
    
    public Path getWorkingDirectory() {
        return this.workingDir;
    }
    
    public String getUsername() {
        return this.username;
    }
    
    public boolean mkdirs(final Path path, final FsPermission permission) throws IOException, FileAlreadyExistsException {
        try {
            return this.innerMkdirs(path, permission);
        }
        catch (AmazonClientException e) {
            throw S3AUtils.translateException("innerMkdirs", path, e);
        }
    }
    
    private DirectoryStatus checkPathForDirectory(final Path path) throws IOException {
        try {
            if (path.isRoot()) {
                return DirectoryStatus.EXISTS_AND_IS_DIRECTORY_ON_METADATASTORE;
            }
            final String key = this.pathToKey(path);
            FileStatus status = null;
            final PathMetadata pm = this.metadataStore.get(path, false);
            if (pm != null) {
                if (pm.isDeleted()) {
                    return DirectoryStatus.DOES_NOT_EXIST;
                }
                status = pm.getFileStatus();
                if (status != null && status.isDirectory()) {
                    return DirectoryStatus.EXISTS_AND_IS_DIRECTORY_ON_METADATASTORE;
                }
            }
            status = this.s3GetFileStatus(path, key, null);
            if (status.isDirectory()) {
                return DirectoryStatus.EXISTS_AND_IS_DIRECTORY_ON_S3_ONLY;
            }
        }
        catch (FileNotFoundException e) {
            return DirectoryStatus.DOES_NOT_EXIST;
        }
        return DirectoryStatus.EXISTS_AND_IS_FILE;
    }
    
    private boolean innerMkdirs(final Path p, final FsPermission permission) throws IOException, FileAlreadyExistsException, AmazonClientException {
        boolean createOnS3 = false;
        final Path f = this.qualify(p);
        S3AFileSystem.LOG.debug("Making directory: {}", (Object)f);
        this.incrementStatistic(Statistic.INVOCATION_MKDIRS);
        List<Path> metadataStoreDirs = null;
        if (this.hasMetadataStore()) {
            metadataStoreDirs = new ArrayList<Path>();
        }
        DirectoryStatus status = this.checkPathForDirectory(f);
        if (status == DirectoryStatus.DOES_NOT_EXIST) {
            createOnS3 = true;
            if (metadataStoreDirs != null) {
                metadataStoreDirs.add(f);
            }
        }
        else if (status == DirectoryStatus.EXISTS_AND_IS_DIRECTORY_ON_S3_ONLY) {
            if (metadataStoreDirs != null) {
                metadataStoreDirs.add(f);
            }
        }
        else {
            if (status == DirectoryStatus.EXISTS_AND_IS_DIRECTORY_ON_METADATASTORE) {
                return true;
            }
            if (status == DirectoryStatus.EXISTS_AND_IS_FILE) {
                throw new FileAlreadyExistsException("Path is a file: " + f);
            }
        }
        Path fPart = f.getParent();
        do {
            status = this.checkPathForDirectory(fPart);
            if (status == DirectoryStatus.DOES_NOT_EXIST || status == DirectoryStatus.EXISTS_AND_IS_DIRECTORY_ON_S3_ONLY) {
                if (metadataStoreDirs != null) {
                    metadataStoreDirs.add(fPart);
                }
            }
            else {
                if (status == DirectoryStatus.EXISTS_AND_IS_DIRECTORY_ON_METADATASTORE) {
                    break;
                }
                if (status == DirectoryStatus.EXISTS_AND_IS_FILE) {
                    throw new FileAlreadyExistsException("Path is a file: " + f);
                }
            }
            fPart = fPart.getParent();
        } while (fPart != null);
        if (createOnS3) {
            final String key = this.pathToKey(f);
            this.createFakeDirectory(key);
        }
        S3Guard.makeDirsOrdered(this.metadataStore, metadataStoreDirs, this.username);
        return true;
    }
    
    public FileStatus getFileStatus(final Path f) throws IOException {
        return this.innerGetFileStatus(f, false);
    }
    
    @VisibleForTesting
    S3AFileStatus innerGetFileStatus(final Path f, final boolean needEmptyDirectoryFlag) throws IOException {
        this.incrementStatistic(Statistic.INVOCATION_GET_FILE_STATUS);
        final Path path = this.qualify(f);
        final String key = this.pathToKey(path);
        S3AFileSystem.LOG.debug("Getting path status for {}  ({})", (Object)path, (Object)key);
        final PathMetadata pm = this.metadataStore.get(path, needEmptyDirectoryFlag);
        Set<Path> tombstones = (Set<Path>)Collections.EMPTY_SET;
        if (pm == null) {
            return S3Guard.putAndReturn(this.metadataStore, this.s3GetFileStatus(path, key, tombstones), this.instrumentation);
        }
        if (pm.isDeleted()) {
            throw new FileNotFoundException("Path " + f + " is recorded as " + "deleted by S3Guard");
        }
        final FileStatus msStatus = pm.getFileStatus();
        if (!needEmptyDirectoryFlag || !msStatus.isDirectory()) {
            return S3AFileStatus.fromFileStatus(msStatus, pm.isEmptyDirectory());
        }
        if (pm.isEmptyDirectory() != Tristate.UNKNOWN) {
            return S3AFileStatus.fromFileStatus(msStatus, pm.isEmptyDirectory());
        }
        final DirListingMetadata children = this.metadataStore.listChildren(path);
        if (children != null) {
            tombstones = children.listTombstones();
        }
        S3AFileSystem.LOG.debug("MetadataStore doesn't know if dir is empty, using S3.");
        S3AFileStatus s3FileStatus;
        try {
            s3FileStatus = this.s3GetFileStatus(path, key, tombstones);
        }
        catch (FileNotFoundException e) {
            return S3AFileStatus.fromFileStatus(msStatus, Tristate.TRUE);
        }
        return S3Guard.putAndReturn(this.metadataStore, s3FileStatus, this.instrumentation);
    }
    
    private S3AFileStatus s3GetFileStatus(final Path path, String key, final Set<Path> tombstones) throws IOException {
        if (!key.isEmpty()) {
            try {
                final ObjectMetadata meta = this.getObjectMetadata(key);
                if (S3AUtils.objectRepresentsDirectory(key, meta.getContentLength())) {
                    S3AFileSystem.LOG.debug("Found exact file: fake directory");
                    return new S3AFileStatus(Tristate.TRUE, path, this.username);
                }
                S3AFileSystem.LOG.debug("Found exact file: normal file");
                return new S3AFileStatus(meta.getContentLength(), S3AUtils.dateToLong(meta.getLastModified()), path, this.getDefaultBlockSize(path), this.username);
            }
            catch (AmazonServiceException e) {
                if (e.getStatusCode() != 404) {
                    throw S3AUtils.translateException("getFileStatus", path, (AmazonClientException)e);
                }
                if (!key.endsWith("/")) {
                    final String newKey = key + "/";
                    try {
                        final ObjectMetadata meta2 = this.getObjectMetadata(newKey);
                        if (S3AUtils.objectRepresentsDirectory(newKey, meta2.getContentLength())) {
                            S3AFileSystem.LOG.debug("Found file (with /): fake directory");
                            return new S3AFileStatus(Tristate.TRUE, path, this.username);
                        }
                        S3AFileSystem.LOG.warn("Found file (with /): real file? should not happen: {}", (Object)key);
                        return new S3AFileStatus(meta2.getContentLength(), S3AUtils.dateToLong(meta2.getLastModified()), path, this.getDefaultBlockSize(path), this.username);
                    }
                    catch (AmazonServiceException e2) {
                        if (e2.getStatusCode() != 404) {
                            throw S3AUtils.translateException("getFileStatus", newKey, (AmazonClientException)e2);
                        }
                    }
                    catch (AmazonClientException e3) {
                        throw S3AUtils.translateException("getFileStatus", newKey, e3);
                    }
                }
            }
            catch (AmazonClientException ex) {}
        }
        try {
            key = this.maybeAddTrailingSlash(key);
            final ListObjectsRequest request = new ListObjectsRequest();
            request.setBucketName(this.bucket);
            request.setPrefix(key);
            request.setDelimiter("/");
            request.setMaxKeys(Integer.valueOf(1));
            final ObjectListing objects = this.listObjects(request);
            final Collection<String> prefixes = (Collection<String>)objects.getCommonPrefixes();
            final Collection<S3ObjectSummary> summaries = (Collection<S3ObjectSummary>)objects.getObjectSummaries();
            if (!this.isEmptyOfKeys(prefixes, tombstones) || !this.isEmptyOfObjects(summaries, tombstones)) {
                if (S3AFileSystem.LOG.isDebugEnabled()) {
                    S3AFileSystem.LOG.debug("Found path as directory (with /): {}/{}", (Object)prefixes.size(), (Object)summaries.size());
                    for (final S3ObjectSummary summary : summaries) {
                        S3AFileSystem.LOG.debug("Summary: {} {}", (Object)summary.getKey(), (Object)summary.getSize());
                    }
                    for (final String prefix : prefixes) {
                        S3AFileSystem.LOG.debug("Prefix: {}", (Object)prefix);
                    }
                }
                return new S3AFileStatus(Tristate.FALSE, path, this.username);
            }
            if (key.isEmpty()) {
                S3AFileSystem.LOG.debug("Found root directory");
                return new S3AFileStatus(Tristate.TRUE, path, this.username);
            }
        }
        catch (AmazonServiceException e) {
            if (e.getStatusCode() != 404) {
                throw S3AUtils.translateException("getFileStatus", key, (AmazonClientException)e);
            }
        }
        catch (AmazonClientException e4) {
            throw S3AUtils.translateException("getFileStatus", key, e4);
        }
        S3AFileSystem.LOG.debug("Not Found: {}", (Object)path);
        throw new FileNotFoundException("No such file or directory: " + path);
    }
    
    private boolean isEmptyOfKeys(final Collection<String> keys, final Set<Path> tombstones) {
        if (tombstones == null) {
            return keys.isEmpty();
        }
        for (final String key : keys) {
            final Path qualified = this.keyToQualifiedPath(key);
            if (!tombstones.contains(qualified)) {
                return false;
            }
        }
        return true;
    }
    
    private boolean isEmptyOfObjects(final Collection<S3ObjectSummary> summaries, final Set<Path> tombstones) {
        if (tombstones == null) {
            return summaries.isEmpty();
        }
        final Collection<String> stringCollection = new ArrayList<String>(summaries.size());
        for (final S3ObjectSummary summary : summaries) {
            stringCollection.add(summary.getKey());
        }
        return this.isEmptyOfKeys(stringCollection, tombstones);
    }
    
    private boolean s3Exists(final Path f) throws IOException {
        final Path path = this.qualify(f);
        final String key = this.pathToKey(path);
        try {
            return this.s3GetFileStatus(path, key, null) != null;
        }
        catch (FileNotFoundException e) {
            return false;
        }
    }
    
    public void copyFromLocalFile(final boolean delSrc, final boolean overwrite, final Path src, final Path dst) throws IOException {
        try {
            this.innerCopyFromLocalFile(delSrc, overwrite, src, dst);
        }
        catch (AmazonClientException e) {
            throw S3AUtils.translateException("copyFromLocalFile(" + src + ", " + dst + ")", src, e);
        }
    }
    
    private void innerCopyFromLocalFile(final boolean delSrc, final boolean overwrite, final Path src, final Path dst) throws IOException, FileAlreadyExistsException, AmazonClientException {
        this.incrementStatistic(Statistic.INVOCATION_COPY_FROM_LOCAL_FILE);
        final String key = this.pathToKey(dst);
        if (!overwrite && this.exists(dst)) {
            throw new FileAlreadyExistsException(dst + " already exists");
        }
        S3AFileSystem.LOG.debug("Copying local file from {} to {}", (Object)src, (Object)dst);
        final LocalFileSystem local = getLocal(this.getConf());
        final File srcfile = local.pathToFile(src);
        final ObjectMetadata om = this.newObjectMetadata(srcfile.length());
        final PutObjectRequest putObjectRequest = this.newPutObjectRequest(key, om, srcfile);
        final UploadInfo info = this.putObject(putObjectRequest);
        final Upload upload = info.getUpload();
        final ProgressableProgressListener listener = new ProgressableProgressListener(this, key, upload, null);
        upload.addProgressListener((ProgressListener)listener);
        try {
            upload.waitForUploadResult();
        }
        catch (InterruptedException e) {
            throw new InterruptedIOException("Interrupted copying " + src + " to " + dst + ", cancelling");
        }
        listener.uploadCompleted();
        this.finishedWrite(key, info.getLength());
        if (delSrc) {
            local.delete(src, false);
        }
    }
    
    public void close() throws IOException {
        if (this.closed.getAndSet(true)) {
            return;
        }
        try {
            super.close();
        }
        finally {
            if (this.transfers != null) {
                this.transfers.shutdownNow(true);
                this.transfers = null;
            }
            if (this.metadataStore != null) {
                this.metadataStore.close();
                this.metadataStore = null;
            }
        }
    }
    
    public String getCanonicalServiceName() {
        return null;
    }
    
    private void copyFile(final String srcKey, final String dstKey, final long size) throws IOException, InterruptedIOException, AmazonClientException {
        S3AFileSystem.LOG.debug("copyFile {} -> {} ", (Object)srcKey, (Object)dstKey);
        try {
            final ObjectMetadata srcom = this.getObjectMetadata(srcKey);
            final ObjectMetadata dstom = this.cloneObjectMetadata(srcom);
            this.setOptionalObjectMetadata(dstom);
            final CopyObjectRequest copyObjectRequest = new CopyObjectRequest(this.bucket, srcKey, this.bucket, dstKey);
            this.setOptionalCopyObjectRequestParameters(copyObjectRequest);
            copyObjectRequest.setCannedAccessControlList(this.cannedACL);
            copyObjectRequest.setNewObjectMetadata(dstom);
            final ProgressListener progressListener = (ProgressListener)new ProgressListener() {
                public void progressChanged(final ProgressEvent progressEvent) {
                    switch (progressEvent.getEventType()) {
                        case TRANSFER_PART_COMPLETED_EVENT: {
                            S3AFileSystem.this.incrementWriteOperations();
                            break;
                        }
                    }
                }
            };
            final Copy copy = this.transfers.copy(copyObjectRequest);
            copy.addProgressListener(progressListener);
            try {
                copy.waitForCopyResult();
                this.incrementWriteOperations();
                this.instrumentation.filesCopied(1, size);
            }
            catch (InterruptedException e2) {
                throw new InterruptedIOException("Interrupted copying " + srcKey + " to " + dstKey + ", cancelling");
            }
        }
        catch (AmazonClientException e) {
            throw S3AUtils.translateException("copyFile(" + srcKey + ", " + dstKey + ")", srcKey, e);
        }
    }
    
    protected void setOptionalMultipartUploadRequestParameters(final InitiateMultipartUploadRequest req) {
        switch (this.serverSideEncryptionAlgorithm) {
            case SSE_KMS: {
                req.setSSEAwsKeyManagementParams(this.generateSSEAwsKeyParams());
                break;
            }
            case SSE_C: {
                if (StringUtils.isNotBlank(S3AUtils.getServerSideEncryptionKey(this.getConf()))) {
                    req.setSSECustomerKey(this.generateSSECustomerKey());
                    break;
                }
                break;
            }
        }
    }
    
    protected void setOptionalCopyObjectRequestParameters(final CopyObjectRequest copyObjectRequest) throws IOException {
        switch (this.serverSideEncryptionAlgorithm) {
            case SSE_KMS: {
                copyObjectRequest.setSSEAwsKeyManagementParams(this.generateSSEAwsKeyParams());
                break;
            }
            case SSE_C: {
                if (StringUtils.isNotBlank(S3AUtils.getServerSideEncryptionKey(this.getConf()))) {
                    final SSECustomerKey customerKey = this.generateSSECustomerKey();
                    copyObjectRequest.setSourceSSECustomerKey(customerKey);
                    copyObjectRequest.setDestinationSSECustomerKey(customerKey);
                    break;
                }
                break;
            }
        }
    }
    
    private void setOptionalPutRequestParameters(final PutObjectRequest request) {
        switch (this.serverSideEncryptionAlgorithm) {
            case SSE_KMS: {
                request.setSSEAwsKeyManagementParams(this.generateSSEAwsKeyParams());
                break;
            }
            case SSE_C: {
                if (StringUtils.isNotBlank(S3AUtils.getServerSideEncryptionKey(this.getConf()))) {
                    request.setSSECustomerKey(this.generateSSECustomerKey());
                    break;
                }
                break;
            }
        }
    }
    
    private void setOptionalObjectMetadata(final ObjectMetadata metadata) {
        if (S3AEncryptionMethods.SSE_S3.equals(this.serverSideEncryptionAlgorithm)) {
            metadata.setSSEAlgorithm(this.serverSideEncryptionAlgorithm.getMethod());
        }
    }
    
    private SSEAwsKeyManagementParams generateSSEAwsKeyParams() {
        SSEAwsKeyManagementParams sseAwsKeyManagementParams = new SSEAwsKeyManagementParams();
        if (StringUtils.isNotBlank(S3AUtils.getServerSideEncryptionKey(this.getConf()))) {
            sseAwsKeyManagementParams = new SSEAwsKeyManagementParams(S3AUtils.getServerSideEncryptionKey(this.getConf()));
        }
        return sseAwsKeyManagementParams;
    }
    
    private SSECustomerKey generateSSECustomerKey() {
        final SSECustomerKey customerKey = new SSECustomerKey(S3AUtils.getServerSideEncryptionKey(this.getConf()));
        return customerKey;
    }
    
    public void finishedWrite(final String key, final long length) {
        S3AFileSystem.LOG.debug("Finished write to {}, len {}", (Object)key, (Object)length);
        final Path p = this.keyToQualifiedPath(key);
        this.deleteUnnecessaryFakeDirectories(p.getParent());
        try {
            if (this.hasMetadataStore()) {
                final S3AFileStatus status = S3AUtils.createUploadFileStatus(p, S3AUtils.objectRepresentsDirectory(key, length), length, this.getDefaultBlockSize(p), this.username);
                S3Guard.putAndReturn(this.metadataStore, status, this.instrumentation);
            }
        }
        catch (IOException e) {
            S3AFileSystem.LOG.error("s3guard: Error updating MetadataStore for write to {}:", (Object)key, (Object)e);
            this.instrumentation.errorIgnored();
        }
    }
    
    private void deleteUnnecessaryFakeDirectories(Path path) {
        final List<DeleteObjectsRequest.KeyVersion> keysToRemove = new ArrayList<DeleteObjectsRequest.KeyVersion>();
        while (!path.isRoot()) {
            String key = this.pathToKey(path);
            key = (key.endsWith("/") ? key : (key + "/"));
            keysToRemove.add(new DeleteObjectsRequest.KeyVersion(key));
            path = path.getParent();
        }
        try {
            this.removeKeys(keysToRemove, false, true);
        }
        catch (AmazonClientException | InvalidRequestException ex2) {

            this.instrumentation.errorIgnored();
            if (S3AFileSystem.LOG.isDebugEnabled()) {
                final StringBuilder sb = new StringBuilder();
                for (final DeleteObjectsRequest.KeyVersion kv : keysToRemove) {
                    sb.append(kv.getKey()).append(",");
                }
                S3AFileSystem.LOG.debug("While deleting keys {} ", (Object)sb.toString(), (Object)ex2);
            }
        }
    }
    
    private void createFakeDirectory(final String objectName) throws AmazonClientException, AmazonServiceException, InterruptedIOException {
        if (!objectName.endsWith("/")) {
            this.createEmptyObject(objectName + "/");
        }
        else {
            this.createEmptyObject(objectName);
        }
    }
    
    private void createEmptyObject(final String objectName) throws AmazonClientException, AmazonServiceException, InterruptedIOException {
        final InputStream im = new InputStream() {
            @Override
            public int read() throws IOException {
                return -1;
            }
        };
        final PutObjectRequest putObjectRequest = this.newPutObjectRequest(objectName, this.newObjectMetadata(0L), im);
        final UploadInfo info = this.putObject(putObjectRequest);
        try {
            info.getUpload().waitForUploadResult();
        }
        catch (InterruptedException e) {
            throw new InterruptedIOException("Interrupted creating " + objectName);
        }
        this.incrementPutProgressStatistics(objectName, 0L);
        this.instrumentation.directoryCreated();
    }
    
    private ObjectMetadata cloneObjectMetadata(final ObjectMetadata source) {
        final ObjectMetadata ret = this.newObjectMetadata(source.getContentLength());
        if (source.getCacheControl() != null) {
            ret.setCacheControl(source.getCacheControl());
        }
        if (source.getContentDisposition() != null) {
            ret.setContentDisposition(source.getContentDisposition());
        }
        if (source.getContentEncoding() != null) {
            ret.setContentEncoding(source.getContentEncoding());
        }
        if (source.getContentMD5() != null) {
            ret.setContentMD5(source.getContentMD5());
        }
        if (source.getContentType() != null) {
            ret.setContentType(source.getContentType());
        }
        if (source.getExpirationTime() != null) {
            ret.setExpirationTime(source.getExpirationTime());
        }
        if (source.getExpirationTimeRuleId() != null) {
            ret.setExpirationTimeRuleId(source.getExpirationTimeRuleId());
        }
        if (source.getHttpExpiresDate() != null) {
            ret.setHttpExpiresDate(source.getHttpExpiresDate());
        }
        if (source.getLastModified() != null) {
            ret.setLastModified(source.getLastModified());
        }
        if (source.getOngoingRestore() != null) {
            ret.setOngoingRestore((boolean)source.getOngoingRestore());
        }
        if (source.getRestoreExpirationTime() != null) {
            ret.setRestoreExpirationTime(source.getRestoreExpirationTime());
        }
        if (source.getSSEAlgorithm() != null) {
            ret.setSSEAlgorithm(source.getSSEAlgorithm());
        }
        if (source.getSSECustomerAlgorithm() != null) {
            ret.setSSECustomerAlgorithm(source.getSSECustomerAlgorithm());
        }
        if (source.getSSECustomerKeyMd5() != null) {
            ret.setSSECustomerKeyMd5(source.getSSECustomerKeyMd5());
        }
        for (final Map.Entry<String, String> e : source.getUserMetadata().entrySet()) {
            ret.addUserMetadata((String)e.getKey(), (String)e.getValue());
        }
        return ret;
    }
    
    @Deprecated
    public long getDefaultBlockSize() {
        return this.getConf().getLongBytes("fs.s3a.block.size", 33554432L);
    }
    
    public String toString() {
        final StringBuilder sb = new StringBuilder("S3AFileSystem{");
        sb.append("uri=").append(this.uri);
        sb.append(", workingDir=").append(this.workingDir);
        sb.append(", inputPolicy=").append(this.inputPolicy);
        sb.append(", partSize=").append(this.partSize);
        sb.append(", enableMultiObjectsDelete=").append(this.enableMultiObjectsDelete);
        sb.append(", maxKeys=").append(this.maxKeys);
        if (this.cannedACL != null) {
            sb.append(", cannedACL=").append(this.cannedACL.toString());
        }
        sb.append(", readAhead=").append(this.readAhead);
        sb.append(", blockSize=").append(this.getDefaultBlockSize());
        sb.append(", multiPartThreshold=").append(this.multiPartThreshold);
        if (this.serverSideEncryptionAlgorithm != null) {
            sb.append(", serverSideEncryptionAlgorithm='").append(this.serverSideEncryptionAlgorithm).append('\'');
        }
        if (this.blockFactory != null) {
            sb.append(", blockFactory=").append(this.blockFactory);
        }
        sb.append(", authoritative=").append(this.allowAuthoritative);
        sb.append(", boundedExecutor=").append(this.boundedThreadPool);
        sb.append(", unboundedExecutor=").append(this.unboundedThreadPool);
        sb.append(", statistics {").append(this.statistics).append("}");
        sb.append(", metrics {").append(this.instrumentation.dump("{", "=", "} ", true)).append("}");
        sb.append('}');
        return sb.toString();
    }
    
    public long getPartitionSize() {
        return this.partSize;
    }
    
    public long getMultiPartThreshold() {
        return this.multiPartThreshold;
    }
    
    int getMaxKeys() {
        return this.maxKeys;
    }
    
    public FileStatus[] globStatus(final Path pathPattern) throws IOException {
        this.incrementStatistic(Statistic.INVOCATION_GLOB_STATUS);
        return super.globStatus(pathPattern);
    }
    
    public FileStatus[] globStatus(final Path pathPattern, final PathFilter filter) throws IOException {
        this.incrementStatistic(Statistic.INVOCATION_GLOB_STATUS);
        return super.globStatus(pathPattern, filter);
    }
    
    public boolean exists(final Path f) throws IOException {
        this.incrementStatistic(Statistic.INVOCATION_EXISTS);
        return super.exists(f);
    }
    
    public boolean isDirectory(final Path f) throws IOException {
        this.incrementStatistic(Statistic.INVOCATION_IS_DIRECTORY);
        return super.isDirectory(f);
    }
    
    public boolean isFile(final Path f) throws IOException {
        this.incrementStatistic(Statistic.INVOCATION_IS_FILE);
        return super.isFile(f);
    }
    
    public RemoteIterator<LocatedFileStatus> listFiles(final Path f, final boolean recursive) throws FileNotFoundException, IOException {
        return this.innerListFiles(f, recursive, new Listing.AcceptFilesOnly(this.qualify(f)));
    }
    
    public RemoteIterator<LocatedFileStatus> listFilesAndEmptyDirectories(final Path f, final boolean recursive) throws IOException {
        return this.innerListFiles(f, recursive, new Listing.AcceptAllButS3nDirs());
    }
    
    private RemoteIterator<LocatedFileStatus> innerListFiles(final Path f, final boolean recursive, final Listing.FileStatusAcceptor acceptor) throws IOException {
        this.incrementStatistic(Statistic.INVOCATION_LIST_FILES);
        final Path path = this.qualify(f);
        S3AFileSystem.LOG.debug("listFiles({}, {})", (Object)path, (Object)recursive);
        try {
            final FileStatus fileStatus = this.getFileStatus(path);
            if (fileStatus.isFile()) {
                S3AFileSystem.LOG.debug("Path is a file");
                return (RemoteIterator<LocatedFileStatus>)new Listing.SingleStatusRemoteIterator(this.toLocatedFileStatus(fileStatus));
            }
            final String key = this.maybeAddTrailingSlash(this.pathToKey(path));
            final String delimiter = recursive ? null : "/";
            S3AFileSystem.LOG.debug("Requesting all entries under {} with delimiter '{}'", (Object)key, (Object)delimiter);
            Set<Path> tombstones;
            RemoteIterator<FileStatus> cachedFilesIterator;
            if (recursive) {
                final PathMetadata pm = this.metadataStore.get(path, true);
                final MetadataStoreListFilesIterator metadataStoreListFilesIterator = new MetadataStoreListFilesIterator(this.metadataStore, pm, this.allowAuthoritative);
                tombstones = metadataStoreListFilesIterator.listTombstones();
                cachedFilesIterator = (RemoteIterator<FileStatus>)metadataStoreListFilesIterator;
            }
            else {
                final DirListingMetadata meta = this.metadataStore.listChildren(path);
                if (meta != null) {
                    tombstones = meta.listTombstones();
                }
                else {
                    tombstones = null;
                }
                cachedFilesIterator = (RemoteIterator<FileStatus>)this.listing.createProvidedFileStatusIterator(S3Guard.dirMetaToStatuses(meta), Listing.ACCEPT_ALL, acceptor);
                if (this.allowAuthoritative && meta != null && meta.isAuthoritative()) {
                    return (RemoteIterator<LocatedFileStatus>)this.listing.createLocatedFileStatusIterator(cachedFilesIterator);
                }
            }
            return (RemoteIterator<LocatedFileStatus>)this.listing.createTombstoneReconcilingIterator((RemoteIterator<LocatedFileStatus>)this.listing.createLocatedFileStatusIterator((RemoteIterator<FileStatus>)this.listing.createFileStatusListingIterator(path, this.createListObjectsRequest(key, delimiter), Listing.ACCEPT_ALL, acceptor, cachedFilesIterator)), tombstones);
        }
        catch (AmazonClientException e) {
            throw S3AUtils.translateException("listFiles", path, e);
        }
    }
    
    public RemoteIterator<LocatedFileStatus> listLocatedStatus(final Path f) throws FileNotFoundException, IOException {
        return this.listLocatedStatus(f, Listing.ACCEPT_ALL);
    }
    
    public RemoteIterator<LocatedFileStatus> listLocatedStatus(final Path f, final PathFilter filter) throws FileNotFoundException, IOException {
        this.incrementStatistic(Statistic.INVOCATION_LIST_LOCATED_STATUS);
        final Path path = this.qualify(f);
        S3AFileSystem.LOG.debug("listLocatedStatus({}, {}", (Object)path, (Object)filter);
        try {
            final FileStatus fileStatus = this.getFileStatus(path);
            if (fileStatus.isFile()) {
                S3AFileSystem.LOG.debug("Path is a file");
                return (RemoteIterator<LocatedFileStatus>)new Listing.SingleStatusRemoteIterator(filter.accept(path) ? this.toLocatedFileStatus(fileStatus) : null);
            }
            final String key = this.maybeAddTrailingSlash(this.pathToKey(path));
            final Listing.FileStatusAcceptor acceptor = new Listing.AcceptAllButSelfAndS3nDirs(path);
            final DirListingMetadata meta = this.metadataStore.listChildren(path);
            final RemoteIterator<FileStatus> cachedFileStatusIterator = (RemoteIterator<FileStatus>)this.listing.createProvidedFileStatusIterator(S3Guard.dirMetaToStatuses(meta), filter, acceptor);
            return (RemoteIterator<LocatedFileStatus>)((this.allowAuthoritative && meta != null && meta.isAuthoritative()) ? this.listing.createLocatedFileStatusIterator(cachedFileStatusIterator) : this.listing.createLocatedFileStatusIterator((RemoteIterator<FileStatus>)this.listing.createFileStatusListingIterator(path, this.createListObjectsRequest(key, "/"), filter, acceptor, cachedFileStatusIterator)));
        }
        catch (AmazonClientException e) {
            throw S3AUtils.translateException("listLocatedStatus", path, e);
        }
    }
    
    LocatedFileStatus toLocatedFileStatus(final FileStatus status) throws IOException {
        return new LocatedFileStatus(status, (BlockLocation[])(status.isFile() ? this.getFileBlockLocations(status, 0L, status.getLen()) : null));
    }
    
    static {
        LOG = LoggerFactory.getLogger((Class)S3AFileSystem.class);
        PROGRESS = LoggerFactory.getLogger("org.apache.hadoop.fs.s3a.S3AFileSystem.Progress");
        warnedOfCoreThreadDeprecation = new AtomicBoolean(false);
        addDeprecatedKeys();
    }
    
    private enum DirectoryStatus
    {
        DOES_NOT_EXIST, 
        EXISTS_AND_IS_DIRECTORY_ON_S3_ONLY, 
        EXISTS_AND_IS_DIRECTORY_ON_METADATASTORE, 
        EXISTS_AND_IS_FILE;
    }
    
    final class WriteOperationHelper
    {
        private final String key;
        
        private WriteOperationHelper(final String key) {
            this.key = key;
        }
        
        PutObjectRequest newPutRequest(final InputStream inputStream, final long length) {
            final PutObjectRequest request = S3AFileSystem.this.newPutObjectRequest(this.key, this.newObjectMetadata(length), inputStream);
            return request;
        }
        
        PutObjectRequest newPutRequest(final File sourceFile) {
            final int length = (int)sourceFile.length();
            final PutObjectRequest request = S3AFileSystem.this.newPutObjectRequest(this.key, this.newObjectMetadata(length), sourceFile);
            return request;
        }
        
        void writeSuccessful(final long length) {
            S3AFileSystem.this.finishedWrite(this.key, length);
        }
        
        void writeFailed(final Exception e) {
            S3AFileSystem.LOG.debug("Write to {} failed", (Object)this, (Object)e);
        }
        
        public ObjectMetadata newObjectMetadata(final long length) {
            return S3AFileSystem.this.newObjectMetadata(length);
        }
        
        String initiateMultiPartUpload() throws IOException {
            S3AFileSystem.LOG.debug("Initiating Multipart upload");
            final InitiateMultipartUploadRequest initiateMPURequest = new InitiateMultipartUploadRequest(S3AFileSystem.this.bucket, this.key, this.newObjectMetadata(-1L));
            initiateMPURequest.setCannedACL(S3AFileSystem.this.cannedACL);
            S3AFileSystem.this.setOptionalMultipartUploadRequestParameters(initiateMPURequest);
            try {
                return S3AFileSystem.this.s3.initiateMultipartUpload(initiateMPURequest).getUploadId();
            }
            catch (AmazonClientException ace) {
                throw S3AUtils.translateException("initiate MultiPartUpload", this.key, ace);
            }
        }
        
        CompleteMultipartUploadResult completeMultipartUpload(final String uploadId, final List<PartETag> partETags) throws AmazonClientException {
            Preconditions.checkNotNull((Object)uploadId);
            Preconditions.checkNotNull((Object)partETags);
            Preconditions.checkArgument(!partETags.isEmpty(), (Object)"No partitions have been uploaded");
            S3AFileSystem.LOG.debug("Completing multipart upload {} with {} parts", (Object)uploadId, (Object)partETags.size());
            return S3AFileSystem.this.s3.completeMultipartUpload(new CompleteMultipartUploadRequest(S3AFileSystem.this.bucket, this.key, uploadId, (List)new ArrayList(partETags)));
        }
        
        void abortMultipartUpload(final String uploadId) throws AmazonClientException {
            S3AFileSystem.LOG.debug("Aborting multipart upload {}", (Object)uploadId);
            S3AFileSystem.this.s3.abortMultipartUpload(new AbortMultipartUploadRequest(S3AFileSystem.this.bucket, this.key, uploadId));
        }
        
        UploadPartRequest newUploadPartRequest(final String uploadId, final int partNumber, final int size, final InputStream uploadStream, final File sourceFile) {
            Preconditions.checkNotNull((Object)uploadId);
            Preconditions.checkArgument(uploadStream != null ^ sourceFile != null, (Object)"Data source");
            Preconditions.checkArgument(size > 0, "Invalid partition size %s", new Object[] { size });
            Preconditions.checkArgument(partNumber > 0 && partNumber <= 10000, "partNumber must be between 1 and 10000 inclusive, but is %s", new Object[] { partNumber });
            S3AFileSystem.LOG.debug("Creating part upload request for {} #{} size {}", new Object[] { uploadId, partNumber, size });
            final UploadPartRequest request = new UploadPartRequest().withBucketName(S3AFileSystem.this.bucket).withKey(this.key).withUploadId(uploadId).withPartNumber(partNumber).withPartSize((long)size);
            if (uploadStream != null) {
                request.setInputStream(uploadStream);
            }
            else {
                request.setFile(sourceFile);
            }
            return request;
        }
        
        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("{bucket=").append(S3AFileSystem.this.bucket);
            sb.append(", key='").append(this.key).append('\'');
            sb.append('}');
            return sb.toString();
        }
        
        PutObjectResult putObject(final PutObjectRequest putObjectRequest) throws IOException {
            try {
                return S3AFileSystem.this.putObjectDirect(putObjectRequest);
            }
            catch (AmazonClientException e) {
                throw S3AUtils.translateException("put", putObjectRequest.getKey(), e);
            }
        }
    }
}
