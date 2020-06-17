// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3;

import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceAudience;

@InterfaceAudience.Public
@InterfaceStability.Stable
@Deprecated
public class VersionMismatchException extends S3FileSystemException
{
    private static final long serialVersionUID = 1L;
    
    public VersionMismatchException(final String clientVersion, final String dataVersion) {
        super("Version mismatch: client expects version " + clientVersion + ", but data has version " + ((dataVersion == null) ? "[unversioned]" : dataVersion));
    }
}
