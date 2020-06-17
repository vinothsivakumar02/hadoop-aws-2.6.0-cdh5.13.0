// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3;

import org.apache.commons.logging.LogFactory;
import java.util.HashMap;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.util.TreeSet;
import java.util.Set;
import java.io.Closeable;
import java.io.OutputStream;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.File;
import java.util.Calendar;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.ServiceException;
import java.io.InputStream;
import org.apache.hadoop.fs.Path;
import org.jets3t.service.S3ServiceException;
import java.io.IOException;
import org.jets3t.service.security.ProviderCredentials;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.security.AWSCredentials;
import java.net.URI;
import org.apache.commons.logging.Log;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.S3Service;
import org.apache.hadoop.conf.Configuration;
import java.util.Map;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceAudience;

@InterfaceAudience.Private
@InterfaceStability.Unstable
@Deprecated
class Jets3tFileSystemStore implements FileSystemStore
{
    private static final String FILE_SYSTEM_NAME = "fs";
    private static final String FILE_SYSTEM_VALUE = "Hadoop";
    private static final String FILE_SYSTEM_TYPE_NAME = "fs-type";
    private static final String FILE_SYSTEM_TYPE_VALUE = "block";
    private static final String FILE_SYSTEM_VERSION_NAME = "fs-version";
    private static final String FILE_SYSTEM_VERSION_VALUE = "1";
    private static final Map<String, Object> METADATA;
    private static final String PATH_DELIMITER = "/";
    private static final String BLOCK_PREFIX = "block_";
    private Configuration conf;
    private S3Service s3Service;
    private S3Bucket bucket;
    private int bufferSize;
    private static final Log LOG;
    
    @Override
    public void initialize(final URI uri, final Configuration conf) throws IOException {
        this.conf = conf;
        final S3Credentials s3Credentials = new S3Credentials();
        s3Credentials.initialize(uri, conf);
        try {
            final AWSCredentials awsCredentials = new AWSCredentials(s3Credentials.getAccessKey(), s3Credentials.getSecretAccessKey());
            this.s3Service = (S3Service)new RestS3Service((ProviderCredentials)awsCredentials);
        }
        catch (S3ServiceException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException)e.getCause();
            }
            throw new S3Exception((Throwable)e);
        }
        this.bucket = new S3Bucket(uri.getHost());
        this.bufferSize = conf.getInt("s3.stream-buffer-size", 4096);
    }
    
    @Override
    public String getVersion() throws IOException {
        return "1";
    }
    
    private void delete(final String key) throws IOException {
        try {
            this.s3Service.deleteObject(this.bucket, key);
        }
        catch (S3ServiceException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException)e.getCause();
            }
            throw new S3Exception((Throwable)e);
        }
    }
    
    @Override
    public void deleteINode(final Path path) throws IOException {
        this.delete(this.pathToKey(path));
    }
    
    @Override
    public void deleteBlock(final Block block) throws IOException {
        this.delete(this.blockToKey(block));
    }
    
    @Override
    public boolean inodeExists(final Path path) throws IOException {
        final InputStream in = this.get(this.pathToKey(path), true);
        if (in == null) {
            return false;
        }
        in.close();
        return true;
    }
    
    @Override
    public boolean blockExists(final long blockId) throws IOException {
        final InputStream in = this.get(this.blockToKey(blockId), false);
        if (in == null) {
            return false;
        }
        in.close();
        return true;
    }
    
    private InputStream get(final String key, final boolean checkMetadata) throws IOException {
        try {
            final S3Object object = this.s3Service.getObject(this.bucket.getName(), key);
            if (checkMetadata) {
                this.checkMetadata(object);
            }
            return object.getDataInputStream();
        }
        catch (S3ServiceException e) {
            if ("NoSuchKey".equals(e.getS3ErrorCode())) {
                return null;
            }
            if (e.getCause() instanceof IOException) {
                throw (IOException)e.getCause();
            }
            throw new S3Exception((Throwable)e);
        }
        catch (ServiceException e2) {
            this.handleServiceException(e2);
            return null;
        }
    }
    
    private InputStream get(final String key, final long byteRangeStart) throws IOException {
        try {
            final S3Object object = this.s3Service.getObject(this.bucket, key, (Calendar)null, (Calendar)null, (String[])null, (String[])null, Long.valueOf(byteRangeStart), (Long)null);
            return object.getDataInputStream();
        }
        catch (S3ServiceException e) {
            if ("NoSuchKey".equals(e.getS3ErrorCode())) {
                return null;
            }
            if (e.getCause() instanceof IOException) {
                throw (IOException)e.getCause();
            }
            throw new S3Exception((Throwable)e);
        }
        catch (ServiceException e2) {
            this.handleServiceException(e2);
            return null;
        }
    }
    
    private void checkMetadata(final S3Object object) throws S3FileSystemException, S3ServiceException {
        final String name = (String)object.getMetadata("fs");
        if (!"Hadoop".equals(name)) {
            throw new S3FileSystemException("Not a Hadoop S3 file.");
        }
        final String type = (String)object.getMetadata("fs-type");
        if (!"block".equals(type)) {
            throw new S3FileSystemException("Not a block file.");
        }
        final String dataVersion = (String)object.getMetadata("fs-version");
        if (!"1".equals(dataVersion)) {
            throw new VersionMismatchException("1", dataVersion);
        }
    }
    
    @Override
    public INode retrieveINode(final Path path) throws IOException {
        return INode.deserialize(this.get(this.pathToKey(path), true));
    }
    
    @Override
    public File retrieveBlock(final Block block, final long byteRangeStart) throws IOException {
        File fileBlock = null;
        InputStream in = null;
        OutputStream out = null;
        try {
            fileBlock = this.newBackupFile();
            in = this.get(this.blockToKey(block), byteRangeStart);
            out = new BufferedOutputStream(new FileOutputStream(fileBlock));
            final byte[] buf = new byte[this.bufferSize];
            int numRead;
            while ((numRead = in.read(buf)) >= 0) {
                out.write(buf, 0, numRead);
            }
            return fileBlock;
        }
        catch (IOException e) {
            this.closeQuietly(out);
            out = null;
            if (fileBlock != null) {
                final boolean b = fileBlock.delete();
                if (!b) {
                    Jets3tFileSystemStore.LOG.warn((Object)"Ignoring failed delete");
                }
            }
            throw e;
        }
        finally {
            this.closeQuietly(out);
            this.closeQuietly(in);
        }
    }
    
    private File newBackupFile() throws IOException {
        final File dir = new File(this.conf.get("fs.s3.buffer.dir"));
        if (!dir.exists() && !dir.mkdirs()) {
            throw new IOException("Cannot create S3 buffer directory: " + dir);
        }
        final File result = File.createTempFile("input-", ".tmp", dir);
        result.deleteOnExit();
        return result;
    }
    
    @Override
    public Set<Path> listSubPaths(final Path path) throws IOException {
        try {
            String prefix = this.pathToKey(path);
            if (!prefix.endsWith("/")) {
                prefix += "/";
            }
            final S3Object[] objects = this.s3Service.listObjects(this.bucket.getName(), prefix, "/");
            final Set<Path> prefixes = new TreeSet<Path>();
            for (int i = 0; i < objects.length; ++i) {
                prefixes.add(this.keyToPath(objects[i].getKey()));
            }
            prefixes.remove(path);
            return prefixes;
        }
        catch (S3ServiceException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException)e.getCause();
            }
            throw new S3Exception((Throwable)e);
        }
    }
    
    @Override
    public Set<Path> listDeepSubPaths(final Path path) throws IOException {
        try {
            String prefix = this.pathToKey(path);
            if (!prefix.endsWith("/")) {
                prefix += "/";
            }
            final S3Object[] objects = this.s3Service.listObjects(this.bucket.getName(), prefix, (String)null);
            final Set<Path> prefixes = new TreeSet<Path>();
            for (int i = 0; i < objects.length; ++i) {
                prefixes.add(this.keyToPath(objects[i].getKey()));
            }
            prefixes.remove(path);
            return prefixes;
        }
        catch (S3ServiceException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException)e.getCause();
            }
            throw new S3Exception((Throwable)e);
        }
    }
    
    private void put(final String key, final InputStream in, final long length, final boolean storeMetadata) throws IOException {
        try {
            final S3Object object = new S3Object(key);
            object.setDataInputStream(in);
            object.setContentType("binary/octet-stream");
            object.setContentLength(length);
            if (storeMetadata) {
                object.addAllMetadata((Map)Jets3tFileSystemStore.METADATA);
            }
            this.s3Service.putObject(this.bucket, object);
        }
        catch (S3ServiceException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException)e.getCause();
            }
            throw new S3Exception((Throwable)e);
        }
    }
    
    @Override
    public void storeINode(final Path path, final INode inode) throws IOException {
        this.put(this.pathToKey(path), inode.serialize(), inode.getSerializedLength(), true);
    }
    
    @Override
    public void storeBlock(final Block block, final File file) throws IOException {
        BufferedInputStream in = null;
        try {
            in = new BufferedInputStream(new FileInputStream(file));
            this.put(this.blockToKey(block), in, block.getLength(), false);
        }
        finally {
            this.closeQuietly(in);
        }
    }
    
    private void closeQuietly(final Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            }
            catch (IOException ex) {}
        }
    }
    
    private String pathToKey(final Path path) {
        if (!path.isAbsolute()) {
            throw new IllegalArgumentException("Path must be absolute: " + path);
        }
        return path.toUri().getPath();
    }
    
    private Path keyToPath(final String key) {
        return new Path(key);
    }
    
    private String blockToKey(final long blockId) {
        return "block_" + blockId;
    }
    
    private String blockToKey(final Block block) {
        return this.blockToKey(block.getId());
    }
    
    @Override
    public void purge() throws IOException {
        try {
            final S3Object[] objects = this.s3Service.listObjects(this.bucket.getName());
            for (int i = 0; i < objects.length; ++i) {
                this.s3Service.deleteObject(this.bucket, objects[i].getKey());
            }
        }
        catch (S3ServiceException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException)e.getCause();
            }
            throw new S3Exception((Throwable)e);
        }
    }
    
    @Override
    public void dump() throws IOException {
        final StringBuilder sb = new StringBuilder("S3 Filesystem, ");
        sb.append(this.bucket.getName()).append("\n");
        try {
            final S3Object[] objects = this.s3Service.listObjects(this.bucket.getName(), "/", (String)null);
            for (int i = 0; i < objects.length; ++i) {
                final Path path = this.keyToPath(objects[i].getKey());
                sb.append(path).append("\n");
                final INode m = this.retrieveINode(path);
                sb.append("\t").append(m.getFileType()).append("\n");
                if (m.getFileType() != INode.FileType.DIRECTORY) {
                    for (int j = 0; j < m.getBlocks().length; ++j) {
                        sb.append("\t").append(m.getBlocks()[j]).append("\n");
                    }
                }
            }
        }
        catch (S3ServiceException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException)e.getCause();
            }
            throw new S3Exception((Throwable)e);
        }
        System.out.println(sb);
    }
    
    private void handleServiceException(final ServiceException e) throws IOException {
        if (e.getCause() instanceof IOException) {
            throw (IOException)e.getCause();
        }
        if (Jets3tFileSystemStore.LOG.isDebugEnabled()) {
            Jets3tFileSystemStore.LOG.debug((Object)("Got ServiceException with Error code: " + e.getErrorCode() + ";and Error message: " + e.getErrorMessage()));
        }
    }
    
    static {
        (METADATA = new HashMap<String, Object>()).put("fs", "Hadoop");
        Jets3tFileSystemStore.METADATA.put("fs-type", "block");
        Jets3tFileSystemStore.METADATA.put("fs-version", "1");
        LOG = LogFactory.getLog(Jets3tFileSystemStore.class.getName());
    }
}
