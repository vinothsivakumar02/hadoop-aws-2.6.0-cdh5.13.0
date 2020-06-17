// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3a;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceAudience;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class AWSServiceIOException extends AWSClientIOException
{
    public AWSServiceIOException(final String operation, final AmazonServiceException cause) {
        super(operation, (AmazonClientException)cause);
    }
    
    public AmazonServiceException getCause() {
        return (AmazonServiceException)super.getCause();
    }
    
    public String getRequestId() {
        return this.getCause().getRequestId();
    }
    
    public String getServiceName() {
        return this.getCause().getServiceName();
    }
    
    public String getErrorCode() {
        return this.getCause().getErrorCode();
    }
    
    public int getStatusCode() {
        return this.getCause().getStatusCode();
    }
    
    public String getRawResponseContent() {
        return this.getCause().getRawResponseContent();
    }
    
    public boolean isRetryable() {
        return this.getCause().isRetryable();
    }
}
