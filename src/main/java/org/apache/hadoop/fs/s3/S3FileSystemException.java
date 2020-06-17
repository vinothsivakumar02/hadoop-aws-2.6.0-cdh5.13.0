// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3;

import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceAudience;
import java.io.IOException;

@InterfaceAudience.Public
@InterfaceStability.Stable
@Deprecated
public class S3FileSystemException extends IOException
{
    private static final long serialVersionUID = 1L;
    
    public S3FileSystemException(final String message) {
        super(message);
    }
}
