// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3a;

import com.amazonaws.AmazonClientException;
import java.util.Map;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceAudience;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class AWSS3IOException extends AWSServiceIOException
{
    public AWSS3IOException(final String operation, final AmazonS3Exception cause) {
        super(operation, (AmazonServiceException)cause);
    }
    
    public AmazonS3Exception getCause() {
        return (AmazonS3Exception)super.getCause();
    }
    
    public String getErrorResponseXml() {
        return this.getCause().getErrorResponseXml();
    }
    
    public Map<String, String> getAdditionalDetails() {
        return (Map<String, String>)this.getCause().getAdditionalDetails();
    }
    
    public String getExtendedRequestId() {
        return this.getCause().getExtendedRequestId();
    }
}
