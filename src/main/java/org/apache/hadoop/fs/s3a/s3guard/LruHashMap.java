// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3a.s3guard;

import java.util.Map;
import java.util.LinkedHashMap;

public class LruHashMap<K, V> extends LinkedHashMap<K, V>
{
    private final int maxSize;
    
    public LruHashMap(final int initialCapacity, final int maxSize) {
        super(initialCapacity);
        this.maxSize = maxSize;
    }
    
    @Override
    protected boolean removeEldestEntry(final Map.Entry<K, V> eldest) {
        return this.size() > this.maxSize;
    }
    
    public V mruGet(final K key) {
        final V val = this.remove(key);
        if (val != null) {
            this.put(key, val);
        }
        return val;
    }
}
