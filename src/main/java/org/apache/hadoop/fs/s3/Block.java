// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3;

import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceAudience;

@InterfaceAudience.Private
@InterfaceStability.Unstable
@Deprecated
public class Block
{
    private long id;
    private long length;
    
    public Block(final long id, final long length) {
        this.id = id;
        this.length = length;
    }
    
    public long getId() {
        return this.id;
    }
    
    public long getLength() {
        return this.length;
    }
    
    @Override
    public String toString() {
        return "Block[" + this.id + ", " + this.length + "]";
    }
}
