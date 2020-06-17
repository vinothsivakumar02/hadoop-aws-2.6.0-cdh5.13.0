// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3;

import java.net.URLDecoder;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import org.jets3t.service.ServiceException;
import java.io.InputStream;
import java.util.TreeSet;
import java.util.Set;
import java.util.Iterator;
import org.apache.hadoop.fs.Path;
import org.jets3t.service.S3ServiceException;
import java.io.IOException;
import org.jets3t.service.security.ProviderCredentials;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.security.AWSCredentials;
import org.jets3t.service.model.S3Object;
import java.net.URI;
import org.apache.hadoop.util.ToolRunner;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.S3Service;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.conf.Configured;

@InterfaceAudience.Public
@InterfaceStability.Unstable
@Deprecated
public class MigrationTool extends Configured implements Tool
{
    private S3Service s3Service;
    private S3Bucket bucket;
    
    public static void main(final String[] args) throws Exception {
        final int res = ToolRunner.run((Tool)new MigrationTool(), args);
        System.exit(res);
    }
    
    public int run(final String[] args) throws Exception {
        if (args.length == 0) {
            System.err.println("Usage: MigrationTool <S3 file system URI>");
            System.err.println("\t<S3 file system URI>\tfilesystem to migrate");
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }
        final URI uri = URI.create(args[0]);
        this.initialize(uri);
        final FileSystemStore newStore = new Jets3tFileSystemStore();
        newStore.initialize(uri, this.getConf());
        if (this.get("%2F") != null) {
            System.err.println("Current version number is [unversioned].");
            System.err.println("Target version number is " + newStore.getVersion() + ".");
            final Store oldStore = new UnversionedStore();
            this.migrate(oldStore, newStore);
            return 0;
        }
        final S3Object root = this.get("/");
        if (root != null) {
            final String version = (String)root.getMetadata("fs-version");
            if (version == null) {
                System.err.println("Can't detect version - exiting.");
            }
            else {
                final String newVersion = newStore.getVersion();
                System.err.println("Current version number is " + version + ".");
                System.err.println("Target version number is " + newVersion + ".");
                if (version.equals(newStore.getVersion())) {
                    System.err.println("No migration required.");
                    return 0;
                }
                System.err.println("Not currently implemented.");
                return 0;
            }
        }
        System.err.println("Can't detect version - exiting.");
        return 0;
    }
    
    public void initialize(final URI uri) throws IOException {
        try {
            String accessKey = null;
            String secretAccessKey = null;
            final String userInfo = uri.getUserInfo();
            if (userInfo != null) {
                final int index = userInfo.indexOf(58);
                if (index != -1) {
                    accessKey = userInfo.substring(0, index);
                    secretAccessKey = userInfo.substring(index + 1);
                }
                else {
                    accessKey = userInfo;
                }
            }
            if (accessKey == null) {
                accessKey = this.getConf().get("fs.s3.awsAccessKeyId");
            }
            if (secretAccessKey == null) {
                secretAccessKey = this.getConf().get("fs.s3.awsSecretAccessKey");
            }
            if (accessKey == null && secretAccessKey == null) {
                throw new IllegalArgumentException("AWS Access Key ID and Secret Access Key must be specified as the username or password (respectively) of a s3 URL, or by setting the fs.s3.awsAccessKeyId or fs.s3.awsSecretAccessKey properties (respectively).");
            }
            if (accessKey == null) {
                throw new IllegalArgumentException("AWS Access Key ID must be specified as the username of a s3 URL, or by setting the fs.s3.awsAccessKeyId property.");
            }
            if (secretAccessKey == null) {
                throw new IllegalArgumentException("AWS Secret Access Key must be specified as the password of a s3 URL, or by setting the fs.s3.awsSecretAccessKey property.");
            }
            final AWSCredentials awsCredentials = new AWSCredentials(accessKey, secretAccessKey);
            this.s3Service = (S3Service)new RestS3Service((ProviderCredentials)awsCredentials);
        }
        catch (S3ServiceException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException)e.getCause();
            }
            throw new S3Exception((Throwable)e);
        }
        this.bucket = new S3Bucket(uri.getHost());
    }
    
    private void migrate(final Store oldStore, final FileSystemStore newStore) throws IOException {
        for (final Path path : oldStore.listAllPaths()) {
            final INode inode = oldStore.retrieveINode(path);
            oldStore.deleteINode(path);
            newStore.storeINode(path, inode);
        }
    }
    
    private S3Object get(final String key) {
        try {
            return this.s3Service.getObject(this.bucket.getName(), key);
        }
        catch (S3ServiceException e) {
            if ("NoSuchKey".equals(e.getS3ErrorCode())) {
                return null;
            }
            return null;
        }
    }
    
    class UnversionedStore implements Store
    {
        @Override
        public Set<Path> listAllPaths() throws IOException {
            try {
                final String prefix = this.urlEncode("/");
                final S3Object[] objects = MigrationTool.this.s3Service.listObjects(MigrationTool.this.bucket.getName(), prefix, (String)null);
                final Set<Path> prefixes = new TreeSet<Path>();
                for (int i = 0; i < objects.length; ++i) {
                    prefixes.add(this.keyToPath(objects[i].getKey()));
                }
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
        public void deleteINode(final Path path) throws IOException {
            this.delete(this.pathToKey(path));
        }
        
        private void delete(final String key) throws IOException {
            try {
                MigrationTool.this.s3Service.deleteObject(MigrationTool.this.bucket, key);
            }
            catch (S3ServiceException e) {
                if (e.getCause() instanceof IOException) {
                    throw (IOException)e.getCause();
                }
                throw new S3Exception((Throwable)e);
            }
        }
        
        @Override
        public INode retrieveINode(final Path path) throws IOException {
            return INode.deserialize(this.get(this.pathToKey(path)));
        }
        
        private InputStream get(final String key) throws IOException {
            try {
                final S3Object object = MigrationTool.this.s3Service.getObject(MigrationTool.this.bucket.getName(), key);
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
                return null;
            }
        }
        
        private String pathToKey(final Path path) {
            if (!path.isAbsolute()) {
                throw new IllegalArgumentException("Path must be absolute: " + path);
            }
            return this.urlEncode(path.toUri().getPath());
        }
        
        private Path keyToPath(final String key) {
            return new Path(this.urlDecode(key));
        }
        
        private String urlEncode(final String s) {
            try {
                return URLEncoder.encode(s, "UTF-8");
            }
            catch (UnsupportedEncodingException e) {
                throw new IllegalStateException(e);
            }
        }
        
        private String urlDecode(final String s) {
            try {
                return URLDecoder.decode(s, "UTF-8");
            }
            catch (UnsupportedEncodingException e) {
                throw new IllegalStateException(e);
            }
        }
    }
    
    interface Store
    {
        Set<Path> listAllPaths() throws IOException;
        
        INode retrieveINode(final Path p0) throws IOException;
        
        void deleteINode(final Path p0) throws IOException;
    }
}
