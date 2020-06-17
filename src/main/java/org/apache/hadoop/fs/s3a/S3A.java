// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3a;

import java.net.URISyntaxException;
import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import java.net.URI;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.DelegateToFileSystem;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class S3A extends DelegateToFileSystem
{
    public S3A(final URI theUri, final Configuration conf) throws IOException, URISyntaxException {
        super(theUri, (FileSystem)new S3AFileSystem(), conf, "s3a", false);
    }
    
    public int getUriDefaultPort() {
        return -1;
    }
}
