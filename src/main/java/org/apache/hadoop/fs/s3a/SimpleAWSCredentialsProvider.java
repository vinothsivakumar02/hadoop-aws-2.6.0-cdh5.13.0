// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3a;

import com.amazonaws.auth.BasicAWSCredentials;
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
public class SimpleAWSCredentialsProvider implements AWSCredentialsProvider
{
    public static final String NAME = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider";
    private String accessKey;
    private String secretKey;
    private IOException lookupIOE;
    
    public SimpleAWSCredentialsProvider(final Configuration conf) {
        try {
            final Configuration c = ProviderUtils.excludeIncompatibleCredentialProviders(conf, (Class)S3AFileSystem.class);
            this.accessKey = S3AUtils.lookupPassword(c, "fs.s3a.access.key", null);
            this.secretKey = S3AUtils.lookupPassword(c, "fs.s3a.secret.key", null);
        }
        catch (IOException e) {
            this.lookupIOE = e;
        }
    }
    
    public AWSCredentials getCredentials() {
        if (this.lookupIOE != null) {
            throw new CredentialInitializationException(this.lookupIOE.toString(), this.lookupIOE);
        }
        if (!StringUtils.isEmpty(this.accessKey) && !StringUtils.isEmpty(this.secretKey)) {
            return (AWSCredentials)new BasicAWSCredentials(this.accessKey, this.secretKey);
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
