// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3;

import java.io.IOException;
import org.apache.hadoop.fs.s3native.S3xLoginHelper;
import org.apache.hadoop.conf.Configuration;
import java.net.URI;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceAudience;

@InterfaceAudience.Private
@InterfaceStability.Unstable
@Deprecated
public class S3Credentials
{
    private String accessKey;
    private String secretAccessKey;
    
    public void initialize(final URI uri, final Configuration conf) throws IOException {
        if (uri.getHost() == null) {
            throw new IllegalArgumentException("Invalid hostname in URI " + uri);
        }
        final S3xLoginHelper.Login login = S3xLoginHelper.extractLoginDetailsWithWarnings(uri);
        if (login.hasLogin()) {
            this.accessKey = login.getUser();
            this.secretAccessKey = login.getPassword();
        }
        final String scheme = uri.getScheme();
        final String accessKeyProperty = String.format("fs.%s.awsAccessKeyId", scheme);
        final String secretAccessKeyProperty = String.format("fs.%s.awsSecretAccessKey", scheme);
        if (this.accessKey == null) {
            this.accessKey = conf.getTrimmed(accessKeyProperty);
        }
        if (this.secretAccessKey == null) {
            final char[] pass = conf.getPassword(secretAccessKeyProperty);
            if (pass != null) {
                this.secretAccessKey = new String(pass).trim();
            }
        }
        if (this.accessKey == null && this.secretAccessKey == null) {
            throw new IllegalArgumentException("AWS Access Key ID and Secret Access Key must be specified by setting the " + accessKeyProperty + " and " + secretAccessKeyProperty + " properties (respectively).");
        }
        if (this.accessKey == null) {
            throw new IllegalArgumentException("AWS Access Key ID must be specified by setting the " + accessKeyProperty + " property.");
        }
        if (this.secretAccessKey == null) {
            throw new IllegalArgumentException("AWS Secret Access Key must be specified by setting the " + secretAccessKeyProperty + " property.");
        }
    }
    
    public String getAccessKey() {
        return this.accessKey;
    }
    
    public String getSecretAccessKey() {
        return this.secretAccessKey;
    }
}
