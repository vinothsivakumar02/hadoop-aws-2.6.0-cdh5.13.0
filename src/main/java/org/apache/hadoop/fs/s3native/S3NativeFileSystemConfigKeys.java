// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3native;

import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.CommonConfigurationKeys;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class S3NativeFileSystemConfigKeys extends CommonConfigurationKeys
{
    public static final String S3_NATIVE_BLOCK_SIZE_KEY = "s3native.blocksize";
    public static final long S3_NATIVE_BLOCK_SIZE_DEFAULT = 67108864L;
    public static final String S3_NATIVE_REPLICATION_KEY = "s3native.replication";
    public static final short S3_NATIVE_REPLICATION_DEFAULT = 1;
    public static final String S3_NATIVE_STREAM_BUFFER_SIZE_KEY = "s3native.stream-buffer-size";
    public static final int S3_NATIVE_STREAM_BUFFER_SIZE_DEFAULT = 4096;
    public static final String S3_NATIVE_BYTES_PER_CHECKSUM_KEY = "s3native.bytes-per-checksum";
    public static final int S3_NATIVE_BYTES_PER_CHECKSUM_DEFAULT = 512;
    public static final String S3_NATIVE_CLIENT_WRITE_PACKET_SIZE_KEY = "s3native.client-write-packet-size";
    public static final int S3_NATIVE_CLIENT_WRITE_PACKET_SIZE_DEFAULT = 65536;
}
