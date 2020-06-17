// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3a;

import com.google.common.base.Preconditions;
import com.amazonaws.AmazonClientException;
import java.io.IOException;

public class AWSClientIOException extends IOException
{
    private final String operation;
    
    public AWSClientIOException(final String operation, final AmazonClientException cause) {
        super((Throwable)cause);
        Preconditions.checkArgument(operation != null, (Object)"Null 'operation' argument");
        Preconditions.checkArgument(cause != null, (Object)"Null 'cause' argument");
        this.operation = operation;
    }
    
    public AmazonClientException getCause() {
        return (AmazonClientException)super.getCause();
    }
    
    @Override
    public String getMessage() {
        return this.operation + ": " + this.getCause().getMessage();
    }
}
