// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3a;

import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceAudience;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;

@InterfaceAudience.Private
@InterfaceStability.Stable
public final class SharedInstanceProfileCredentialsProvider extends InstanceProfileCredentialsProvider
{
    private static final SharedInstanceProfileCredentialsProvider INSTANCE;
    
    public static SharedInstanceProfileCredentialsProvider getInstance() {
        return SharedInstanceProfileCredentialsProvider.INSTANCE;
    }
    
    private SharedInstanceProfileCredentialsProvider() {
    }
    
    static {
        INSTANCE = new SharedInstanceProfileCredentialsProvider();
    }
}
