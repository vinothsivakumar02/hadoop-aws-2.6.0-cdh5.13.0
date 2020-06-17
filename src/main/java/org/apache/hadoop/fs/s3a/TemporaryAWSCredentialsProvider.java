// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3a;

import com.amazonaws.auth.BasicSessionCredentials;
import org.apache.commons.lang.StringUtils;
import com.amazonaws.auth.AWSCredentials;
import org.apache.hadoop.security.ProviderUtils;
import org.apache.hadoop.conf.Configuration;
import java.io.IOException;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceAudience;
import com.amazonaws.auth.AWSCredentialsProvider;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class TemporaryAWSCredentialsProvider implements AWSCredentialsProvider
{
    public static final String NAME = "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider";
    private String accessKey;
    private String secretKey;
    private String sessionToken;
    private IOException lookupIOE;
    
    public TemporaryAWSCredentialsProvider(final Configuration conf) {
        try {
            final Configuration c = ProviderUtils.excludeIncompatibleCredentialProviders(conf, (Class)S3AFileSystem.class);
            this.accessKey = S3AUtils.lookupPassword(c, "fs.s3a.access.key", null);
            this.secretKey = S3AUtils.lookupPassword(c, "fs.s3a.secret.key", null);
            this.sessionToken = S3AUtils.lookupPassword(c, "fs.s3a.session.token", null);
        }
        catch (IOException e) {
            this.lookupIOE = e;
        }
    }
    
    public AWSCredentials getCredentials() {
        if (this.lookupIOE != null) {
            throw new CredentialInitializationException(this.lookupIOE.toString(), this.lookupIOE);
        }
        if (!StringUtils.isEmpty(this.accessKey) && !StringUtils.isEmpty(this.secretKey) && !StringUtils.isEmpty(this.sessionToken)) {
            return (AWSCredentials)new BasicSessionCredentials(this.accessKey, this.secretKey, this.sessionToken);
        }
        throw new CredentialInitializationException("Access key, secret key or session token is unset");
    }
    
    public void refresh() {
    }
    
    @Override
    public String toString() {
        return this.getClass().getSimpleName();
    }
}
