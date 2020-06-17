// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3a;

import com.amazonaws.AmazonClientException;

public class CredentialInitializationException extends AmazonClientException
{
    public CredentialInitializationException(final String message, final Throwable t) {
        super(message, t);
    }
    
    public CredentialInitializationException(final String message) {
        super(message);
    }
    
    public boolean isRetryable() {
        return false;
    }
}
