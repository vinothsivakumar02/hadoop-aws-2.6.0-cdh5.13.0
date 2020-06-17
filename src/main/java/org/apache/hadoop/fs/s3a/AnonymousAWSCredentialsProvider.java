// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3a;

import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.auth.AWSCredentials;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceAudience;
import com.amazonaws.auth.AWSCredentialsProvider;

@InterfaceAudience.Private
@InterfaceStability.Stable
public class AnonymousAWSCredentialsProvider implements AWSCredentialsProvider
{
    public static final String NAME = "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider";
    
    public AWSCredentials getCredentials() {
        return (AWSCredentials)new AnonymousAWSCredentials();
    }
    
    public void refresh() {
    }
    
    @Override
    public String toString() {
        return this.getClass().getSimpleName();
    }
}
