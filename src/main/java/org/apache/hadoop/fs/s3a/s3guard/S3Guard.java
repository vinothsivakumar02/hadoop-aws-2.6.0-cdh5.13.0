// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3a.s3guard;

import org.slf4j.LoggerFactory;
import java.net.URI;
import org.apache.hadoop.fs.s3a.S3AUtils;
import java.util.Set;
import org.apache.hadoop.fs.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Collection;
import java.util.ArrayList;
import org.apache.hadoop.fs.s3a.Statistic;
import org.apache.hadoop.fs.s3a.S3AInstrumentation;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.conf.Configuration;
import java.io.IOException;
import org.apache.hadoop.util.ReflectionUtils;
import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceAudience;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class S3Guard
{
    private static final Logger LOG;
    @InterfaceAudience.Private
    @InterfaceStability.Unstable
    @VisibleForTesting
    public static final String S3GUARD_DDB_CLIENT_FACTORY_IMPL = "fs.s3a.s3guard.ddb.client.factory.impl";
    static final Class<? extends DynamoDBClientFactory> S3GUARD_DDB_CLIENT_FACTORY_IMPL_DEFAULT;
    private static final FileStatus[] EMPTY_LISTING;
    
    private S3Guard() {
    }
    
    public static MetadataStore getMetadataStore(final FileSystem fs) throws IOException {
        Preconditions.checkNotNull((Object)fs);
        final Configuration conf = fs.getConf();
        Preconditions.checkNotNull((Object)conf);
        try {
            final Class<? extends MetadataStore> msClass = getMetadataStoreClass(conf);
            final MetadataStore msInstance = (MetadataStore)ReflectionUtils.newInstance((Class)msClass, conf);
            S3Guard.LOG.debug("Using {} metadata store for {} filesystem", (Object)msClass.getSimpleName(), (Object)fs.getScheme());
            msInstance.initialize(fs);
            return msInstance;
        }
        catch (RuntimeException | IOException e) {

            final String message = "Failed to instantiate metadata store " + conf.get("fs.s3a.metadatastore.impl") + " defined in " + "fs.s3a.metadatastore.impl" + ": " + e;
            S3Guard.LOG.error(message, (Throwable)e);
            if (e instanceof IOException) {
                throw e;
            }
            throw new IOException(message, e);
        }
    }
    
    private static Class<? extends MetadataStore> getMetadataStoreClass(final Configuration conf) {
        if (conf == null) {
            return NullMetadataStore.class;
        }
        return (Class<? extends MetadataStore>)conf.getClass("fs.s3a.metadatastore.impl", (Class)NullMetadataStore.class, (Class)MetadataStore.class);
    }
    
    public static S3AFileStatus putAndReturn(final MetadataStore ms, final S3AFileStatus status, final S3AInstrumentation instrumentation) throws IOException {
        final long startTimeNano = System.nanoTime();
        ms.put(new PathMetadata(status));
        instrumentation.addValueToQuantiles(Statistic.S3GUARD_METADATASTORE_PUT_PATH_LATENCY, System.nanoTime() - startTimeNano);
        instrumentation.incrementCounter(Statistic.S3GUARD_METADATASTORE_PUT_PATH_REQUEST, 1L);
        return status;
    }
    
    public static FileStatus[] dirMetaToStatuses(final DirListingMetadata dirMeta) {
        if (dirMeta == null) {
            return S3Guard.EMPTY_LISTING;
        }
        final Collection<PathMetadata> listing = dirMeta.getListing();
        final List<FileStatus> statuses = new ArrayList<FileStatus>();
        for (final PathMetadata pm : listing) {
            if (!pm.isDeleted()) {
                statuses.add(pm.getFileStatus());
            }
        }
        return statuses.toArray(new FileStatus[0]);
    }
    
    public static FileStatus[] dirListingUnion(final MetadataStore ms, final Path path, final List<FileStatus> backingStatuses, DirListingMetadata dirMeta, final boolean isAuthoritative) throws IOException {
        if (ms instanceof NullMetadataStore) {
            return backingStatuses.toArray(new FileStatus[backingStatuses.size()]);
        }
        assertQualified(path);
        if (dirMeta == null) {
            dirMeta = new DirListingMetadata(path, DirListingMetadata.EMPTY_DIR, false);
        }
        final Set<Path> deleted = dirMeta.listTombstones();
        boolean changed = false;
        for (final FileStatus s : backingStatuses) {
            if (deleted.contains(s.getPath())) {
                continue;
            }
            final boolean updated = dirMeta.put(s);
            changed = (changed || updated);
        }
        if (changed && isAuthoritative) {
            dirMeta.setAuthoritative(true);
            ms.put(dirMeta);
        }
        return dirMetaToStatuses(dirMeta);
    }
    
    public static boolean isNullMetadataStore(final MetadataStore ms) {
        return ms instanceof NullMetadataStore;
    }
    
    public static void makeDirsOrdered(final MetadataStore ms, final List<Path> dirs, final String owner) {
        if (dirs == null) {
            return;
        }
        FileStatus prevStatus = null;
        for (int i = 0; i < dirs.size(); ++i) {
            final boolean isLeaf = prevStatus == null;
            final Path f = dirs.get(i);
            assertQualified(f);
            final FileStatus status = S3AUtils.createUploadFileStatus(f, true, 0L, 0L, owner);
            Collection<PathMetadata> children;
            if (isLeaf) {
                children = DirListingMetadata.EMPTY_DIR;
            }
            else {
                children = new ArrayList<PathMetadata>(1);
                children.add(new PathMetadata(prevStatus));
            }
            final DirListingMetadata dirMeta = new DirListingMetadata(f, children, true);
            try {
                ms.put(dirMeta);
                ms.put(new PathMetadata(status));
            }
            catch (IOException ioe) {
                S3Guard.LOG.error("MetadataStore#put() failure:", (Throwable)ioe);
                return;
            }
            prevStatus = status;
        }
    }
    
    public static void addMoveDir(final MetadataStore ms, final Collection<Path> srcPaths, final Collection<PathMetadata> dstMetas, final Path srcPath, final Path dstPath, final String owner) {
        if (isNullMetadataStore(ms)) {
            return;
        }
        assertQualified(srcPath);
        assertQualified(dstPath);
        final FileStatus dstStatus = S3AUtils.createUploadFileStatus(dstPath, true, 0L, 0L, owner);
        addMoveStatus(srcPaths, dstMetas, srcPath, dstStatus);
    }
    
    public static void addMoveFile(final MetadataStore ms, final Collection<Path> srcPaths, final Collection<PathMetadata> dstMetas, final Path srcPath, final Path dstPath, final long size, final long blockSize, final String owner) {
        if (isNullMetadataStore(ms)) {
            return;
        }
        assertQualified(srcPath);
        assertQualified(dstPath);
        final FileStatus dstStatus = S3AUtils.createUploadFileStatus(dstPath, false, size, blockSize, owner);
        addMoveStatus(srcPaths, dstMetas, srcPath, dstStatus);
    }
    
    public static void addMoveAncestors(final MetadataStore ms, final Collection<Path> srcPaths, final Collection<PathMetadata> dstMetas, final Path srcRoot, final Path srcPath, final Path dstPath, final String owner) {
        if (isNullMetadataStore(ms)) {
            return;
        }
        assertQualified(srcRoot);
        assertQualified(srcPath);
        assertQualified(dstPath);
        if (srcPath.equals((Object)srcRoot)) {
            S3Guard.LOG.debug("Skip moving ancestors of source root directory {}", (Object)srcRoot);
            return;
        }
        for (Path parentSrc = srcPath.getParent(), parentDst = dstPath.getParent(); parentSrc != null && !parentSrc.isRoot() && !parentSrc.equals((Object)srcRoot) && !srcPaths.contains(parentSrc); parentSrc = parentSrc.getParent(), parentDst = parentDst.getParent()) {
            S3Guard.LOG.debug("Renaming non-listed parent {} to {}", (Object)parentSrc, (Object)parentDst);
            addMoveDir(ms, srcPaths, dstMetas, parentSrc, parentDst, owner);
        }
    }
    
    private static void addMoveStatus(final Collection<Path> srcPaths, final Collection<PathMetadata> dstMetas, final Path srcPath, final FileStatus dstStatus) {
        srcPaths.add(srcPath);
        dstMetas.add(new PathMetadata(dstStatus));
    }
    
    public static void assertQualified(final Path p) {
        final URI uri = p.toUri();
        Preconditions.checkNotNull((Object)uri.getHost(), (Object)("Null host in " + uri));
        Preconditions.checkNotNull((Object)uri.getScheme(), (Object)("Null scheme in " + uri));
    }
    
    static {
        LOG = LoggerFactory.getLogger((Class)S3Guard.class);
        S3GUARD_DDB_CLIENT_FACTORY_IMPL_DEFAULT = DynamoDBClientFactory.DefaultDynamoDBClientFactory.class;
        EMPTY_LISTING = new FileStatus[0];
    }
}
