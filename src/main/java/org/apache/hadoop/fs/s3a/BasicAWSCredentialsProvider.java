// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3a;

import com.amazonaws.auth.BasicAWSCredentials;
import org.apache.commons.lang.StringUtils;
import com.amazonaws.auth.AWSCredentials;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceAudience;
import com.amazonaws.auth.AWSCredentialsProvider;

@InterfaceAudience.Private
@InterfaceStability.Stable
public class BasicAWSCredentialsProvider implements AWSCredentialsProvider
{
    public static final String NAME = "org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider";
    private final String accessKey;
    private final String secretKey;
    
    public BasicAWSCredentialsProvider(final String accessKey, final String secretKey) {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
    }
    
    public AWSCredentials getCredentials() {
        if (!StringUtils.isEmpty(this.accessKey) && !StringUtils.isEmpty(this.secretKey)) {
            return (AWSCredentials)new BasicAWSCredentials(this.accessKey, this.secretKey);
        }
        throw new CredentialInitializationException("Access key or secret key is null");
    }
    
    public void refresh() {
    }
    
    @Override
    public String toString() {
        return this.getClass().getSimpleName();
    }
}
