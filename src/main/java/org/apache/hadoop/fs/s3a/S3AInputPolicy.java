// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3a;

import org.slf4j.LoggerFactory;
import java.util.Locale;
import org.slf4j.Logger;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceAudience;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public enum S3AInputPolicy
{
    Normal("normal"), 
    Sequential("sequential"), 
    Random("random");
    
    private static final Logger LOG;
    private final String policy;
    
    private S3AInputPolicy(final String policy) {
        this.policy = policy;
    }
    
    @Override
    public String toString() {
        return this.policy;
    }
    
    public static S3AInputPolicy getPolicy(final String name) {
        final String lowerCase;
        final String trimmed = lowerCase = name.trim().toLowerCase(Locale.ENGLISH);
        switch (lowerCase) {
            case "normal": {
                return S3AInputPolicy.Normal;
            }
            case "random": {
                return S3AInputPolicy.Random;
            }
            case "sequential": {
                return S3AInputPolicy.Sequential;
            }
            default: {
                S3AInputPolicy.LOG.warn("Unrecognized fs.s3a.experimental.input.fadvise value: \"{}\"", (Object)trimmed);
                return S3AInputPolicy.Normal;
            }
        }
    }
    
    static {
        LOG = LoggerFactory.getLogger((Class)S3AInputPolicy.class);
    }
}
