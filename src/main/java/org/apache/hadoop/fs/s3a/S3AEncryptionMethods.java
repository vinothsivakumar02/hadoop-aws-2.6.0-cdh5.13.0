// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3a;

import java.io.IOException;
import org.apache.commons.lang.StringUtils;

public enum S3AEncryptionMethods
{
    SSE_S3("AES256"), 
    SSE_KMS("SSE-KMS"), 
    SSE_C("SSE-C"), 
    NONE("");
    
    static final String UNKNOWN_ALGORITHM = "Unknown Server Side Encryption algorithm ";
    private String method;
    
    private S3AEncryptionMethods(final String method) {
        this.method = method;
    }
    
    public String getMethod() {
        return this.method;
    }
    
    public static S3AEncryptionMethods getMethod(final String name) throws IOException {
        if (StringUtils.isBlank(name)) {
            return S3AEncryptionMethods.NONE;
        }
        switch (name) {
            case "AES256": {
                return S3AEncryptionMethods.SSE_S3;
            }
            case "SSE-KMS": {
                return S3AEncryptionMethods.SSE_KMS;
            }
            case "SSE-C": {
                return S3AEncryptionMethods.SSE_C;
            }
            default: {
                throw new IOException("Unknown Server Side Encryption algorithm " + name);
            }
        }
    }
}
