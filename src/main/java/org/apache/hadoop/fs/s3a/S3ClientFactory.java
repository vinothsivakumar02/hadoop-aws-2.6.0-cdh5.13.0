// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3a;

import java.io.IOException;
import com.amazonaws.services.s3.AmazonS3;
import java.net.URI;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceAudience;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface S3ClientFactory
{
    AmazonS3 createS3Client(final URI p0) throws IOException;
}
