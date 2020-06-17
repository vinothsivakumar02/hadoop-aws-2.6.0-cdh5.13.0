// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3a;

public enum Tristate
{
    TRUE, 
    FALSE, 
    UNKNOWN;
    
    public static Tristate fromBool(final boolean v) {
        return v ? Tristate.TRUE : Tristate.FALSE;
    }
}
