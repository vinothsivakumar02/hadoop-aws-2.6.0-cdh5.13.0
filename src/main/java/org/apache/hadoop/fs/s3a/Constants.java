// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3a;

import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceAudience;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class Constants
{
    public static final int MULTIPART_MIN_SIZE = 5242880;
    public static final String ACCESS_KEY = "fs.s3a.access.key";
    public static final String SECRET_KEY = "fs.s3a.secret.key";
    public static final String AWS_CREDENTIALS_PROVIDER = "fs.s3a.aws.credentials.provider";
    public static final String S3A_SECURITY_CREDENTIAL_PROVIDER_PATH = "fs.s3a.security.credential.provider.path";
    public static final String SESSION_TOKEN = "fs.s3a.session.token";
    public static final String MAXIMUM_CONNECTIONS = "fs.s3a.connection.maximum";
    public static final int DEFAULT_MAXIMUM_CONNECTIONS = 15;
    public static final String SECURE_CONNECTIONS = "fs.s3a.connection.ssl.enabled";
    public static final boolean DEFAULT_SECURE_CONNECTIONS = true;
    public static final String ENDPOINT = "fs.s3a.endpoint";
    public static final String PATH_STYLE_ACCESS = "fs.s3a.path.style.access";
    public static final String PROXY_HOST = "fs.s3a.proxy.host";
    public static final String PROXY_PORT = "fs.s3a.proxy.port";
    public static final String PROXY_USERNAME = "fs.s3a.proxy.username";
    public static final String PROXY_PASSWORD = "fs.s3a.proxy.password";
    public static final String PROXY_DOMAIN = "fs.s3a.proxy.domain";
    public static final String PROXY_WORKSTATION = "fs.s3a.proxy.workstation";
    public static final String MAX_ERROR_RETRIES = "fs.s3a.attempts.maximum";
    public static final int DEFAULT_MAX_ERROR_RETRIES = 20;
    public static final String ESTABLISH_TIMEOUT = "fs.s3a.connection.establish.timeout";
    public static final int DEFAULT_ESTABLISH_TIMEOUT = 50000;
    public static final String SOCKET_TIMEOUT = "fs.s3a.connection.timeout";
    public static final int DEFAULT_SOCKET_TIMEOUT = 200000;
    public static final String SOCKET_SEND_BUFFER = "fs.s3a.socket.send.buffer";
    public static final int DEFAULT_SOCKET_SEND_BUFFER = 8192;
    public static final String SOCKET_RECV_BUFFER = "fs.s3a.socket.recv.buffer";
    public static final int DEFAULT_SOCKET_RECV_BUFFER = 8192;
    public static final String MAX_PAGING_KEYS = "fs.s3a.paging.maximum";
    public static final int DEFAULT_MAX_PAGING_KEYS = 5000;
    public static final String MAX_THREADS = "fs.s3a.threads.max";
    public static final int DEFAULT_MAX_THREADS = 10;
    @Deprecated
    public static final String CORE_THREADS = "fs.s3a.threads.core";
    public static final String KEEPALIVE_TIME = "fs.s3a.threads.keepalivetime";
    public static final int DEFAULT_KEEPALIVE_TIME = 60;
    public static final String MAX_TOTAL_TASKS = "fs.s3a.max.total.tasks";
    public static final int DEFAULT_MAX_TOTAL_TASKS = 5;
    public static final String MULTIPART_SIZE = "fs.s3a.multipart.size";
    public static final long DEFAULT_MULTIPART_SIZE = 67108864L;
    public static final String MIN_MULTIPART_THRESHOLD = "fs.s3a.multipart.threshold";
    public static final long DEFAULT_MIN_MULTIPART_THRESHOLD = 134217728L;
    public static final String ENABLE_MULTI_DELETE = "fs.s3a.multiobjectdelete.enable";
    public static final String BUFFER_DIR = "fs.s3a.buffer.dir";
    public static final String FAST_UPLOAD = "fs.s3a.fast.upload";
    public static final boolean DEFAULT_FAST_UPLOAD = false;
    @Deprecated
    public static final String FAST_BUFFER_SIZE = "fs.s3a.fast.buffer.size";
    public static final int DEFAULT_FAST_BUFFER_SIZE = 1048576;
    @InterfaceStability.Unstable
    public static final String FAST_UPLOAD_BUFFER = "fs.s3a.fast.upload.buffer";
    @InterfaceStability.Unstable
    public static final String FAST_UPLOAD_BUFFER_DISK = "disk";
    @InterfaceStability.Unstable
    public static final String FAST_UPLOAD_BUFFER_ARRAY = "array";
    @InterfaceStability.Unstable
    public static final String FAST_UPLOAD_BYTEBUFFER = "bytebuffer";
    @InterfaceStability.Unstable
    public static final String DEFAULT_FAST_UPLOAD_BUFFER = "disk";
    @InterfaceStability.Unstable
    public static final String FAST_UPLOAD_ACTIVE_BLOCKS = "fs.s3a.fast.upload.active.blocks";
    @InterfaceStability.Unstable
    public static final int DEFAULT_FAST_UPLOAD_ACTIVE_BLOCKS = 4;
    public static final String CANNED_ACL = "fs.s3a.acl.default";
    public static final String DEFAULT_CANNED_ACL = "";
    public static final String PURGE_EXISTING_MULTIPART = "fs.s3a.multipart.purge";
    public static final boolean DEFAULT_PURGE_EXISTING_MULTIPART = false;
    public static final String PURGE_EXISTING_MULTIPART_AGE = "fs.s3a.multipart.purge.age";
    public static final long DEFAULT_PURGE_EXISTING_MULTIPART_AGE = 86400L;
    public static final String SERVER_SIDE_ENCRYPTION_ALGORITHM = "fs.s3a.server-side-encryption-algorithm";
    @Deprecated
    public static final String SERVER_SIDE_ENCRYPTION_AES256 = "AES256";
    public static final String SERVER_SIDE_ENCRYPTION_KEY = "fs.s3a.server-side-encryption.key";
    static final String OLD_S3A_SERVER_SIDE_ENCRYPTION_KEY = "fs.s3a.server-side-encryption-key";
    public static final String SIGNING_ALGORITHM = "fs.s3a.signing-algorithm";
    public static final String S3N_FOLDER_SUFFIX = "_$folder$";
    public static final String FS_S3A_BLOCK_SIZE = "fs.s3a.block.size";
    public static final String FS_S3A = "s3a";
    public static final String FS_S3A_PREFIX = "fs.s3a.";
    public static final String FS_S3A_BUCKET_PREFIX = "fs.s3a.bucket.";
    public static final int S3A_DEFAULT_PORT = -1;
    public static final String USER_AGENT_PREFIX = "fs.s3a.user.agent.prefix";
    public static final String METADATASTORE_AUTHORITATIVE = "fs.s3a.metadatastore.authoritative";
    public static final boolean DEFAULT_METADATASTORE_AUTHORITATIVE = false;
    public static final String READAHEAD_RANGE = "fs.s3a.readahead.range";
    public static final long DEFAULT_READAHEAD_RANGE = 65536L;
    @InterfaceStability.Unstable
    public static final String INPUT_FADVISE = "fs.s3a.experimental.input.fadvise";
    @InterfaceStability.Unstable
    public static final String INPUT_FADV_NORMAL = "normal";
    @InterfaceStability.Unstable
    public static final String INPUT_FADV_SEQUENTIAL = "sequential";
    @InterfaceStability.Unstable
    public static final String INPUT_FADV_RANDOM = "random";
    @InterfaceAudience.Private
    @InterfaceStability.Unstable
    public static final String S3_CLIENT_FACTORY_IMPL = "fs.s3a.s3.client.factory.impl";
    @InterfaceAudience.Private
    @InterfaceStability.Unstable
    public static final Class<? extends S3ClientFactory> DEFAULT_S3_CLIENT_FACTORY_IMPL;
    @InterfaceAudience.Private
    public static final int MAX_MULTIPART_COUNT = 10000;
    @InterfaceStability.Unstable
    public static final String S3A_OUTPUT_COMMITTER_FACTORY = "org.apache.hadoop.fs.s3a.commit.S3AOutputCommitterFactory";
    public static final String S3_METADATA_STORE_IMPL = "fs.s3a.metadatastore.impl";
    @InterfaceStability.Unstable
    public static final String S3GUARD_CLI_PRUNE_AGE = "fs.s3a.s3guard.cli.prune.age";
    @InterfaceStability.Unstable
    public static final String S3GUARD_DDB_REGION_KEY = "fs.s3a.s3guard.ddb.region";
    @InterfaceStability.Unstable
    public static final String S3GUARD_DDB_TABLE_NAME_KEY = "fs.s3a.s3guard.ddb.table";
    @InterfaceStability.Unstable
    public static final String S3GUARD_DDB_TABLE_CREATE_KEY = "fs.s3a.s3guard.ddb.table.create";
    @InterfaceStability.Unstable
    public static final String S3GUARD_DDB_TABLE_CAPACITY_READ_KEY = "fs.s3a.s3guard.ddb.table.capacity.read";
    public static final long S3GUARD_DDB_TABLE_CAPACITY_READ_DEFAULT = 500L;
    @InterfaceStability.Unstable
    public static final String S3GUARD_DDB_TABLE_CAPACITY_WRITE_KEY = "fs.s3a.s3guard.ddb.table.capacity.write";
    public static final long S3GUARD_DDB_TABLE_CAPACITY_WRITE_DEFAULT = 100L;
    public static final int S3GUARD_DDB_BATCH_WRITE_REQUEST_LIMIT = 25;
    @InterfaceStability.Unstable
    public static final String S3GUARD_DDB_MAX_RETRIES = "fs.s3a.s3guard.ddb.max.retries";
    public static final int S3GUARD_DDB_MAX_RETRIES_DEFAULT = 9;
    @InterfaceStability.Unstable
    public static final String S3GUARD_DDB_BACKGROUND_SLEEP_MSEC_KEY = "fs.s3a.s3guard.ddb.background.sleep";
    public static final int S3GUARD_DDB_BACKGROUND_SLEEP_MSEC_DEFAULT = 25;
    @InterfaceStability.Unstable
    public static final String S3A_OUTPUT_COMMITTER_MRV1 = "org.apache.hadoop.fs.s3a.commit.S3OutputCommitterMRv1";
    @InterfaceStability.Unstable
    public static final String S3GUARD_METASTORE_NULL = "org.apache.hadoop.fs.s3a.s3guard.NullMetadataStore";
    @InterfaceStability.Unstable
    public static final String S3GUARD_METASTORE_LOCAL = "org.apache.hadoop.fs.s3a.s3guard.LocalMetadataStore";
    @InterfaceStability.Unstable
    public static final String S3GUARD_METASTORE_DYNAMO = "org.apache.hadoop.fs.s3a.s3guard.DynamoDBMetadataStore";
    
    private Constants() {
    }
    
    static {
        DEFAULT_S3_CLIENT_FACTORY_IMPL = DefaultS3ClientFactory.class;
    }
}
