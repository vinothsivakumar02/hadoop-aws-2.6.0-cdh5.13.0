// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3a;

import java.util.NoSuchElementException;
import java.util.Set;
import java.util.Collections;
import java.util.Iterator;
import java.util.EnumMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Map;
import org.slf4j.Logger;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.StorageStatistics;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class S3AStorageStatistics extends StorageStatistics
{
    private static final Logger LOG;
    public static final String NAME = "S3AStorageStatistics";
    private final Map<Statistic, AtomicLong> opsCount;
    
    public S3AStorageStatistics() {
        super("S3AStorageStatistics");
        this.opsCount = new EnumMap<Statistic, AtomicLong>(Statistic.class);
        for (final Statistic opType : Statistic.values()) {
            this.opsCount.put(opType, new AtomicLong(0L));
        }
    }
    
    public long incrementCounter(final Statistic op, final long count) {
        final long updated = this.opsCount.get(op).addAndGet(count);
        S3AStorageStatistics.LOG.debug("{} += {}  ->  {}", new Object[] { op, count, updated });
        return updated;
    }
    
    public String getScheme() {
        return "s3a";
    }
    
    public Iterator<StorageStatistics.LongStatistic> getLongStatistics() {
        return new LongIterator();
    }
    
    public Long getLong(final String key) {
        final Statistic type = Statistic.fromSymbol(key);
        return (type == null) ? null : Long.valueOf(this.opsCount.get(type).get());
    }
    
    public boolean isTracked(final String key) {
        return Statistic.fromSymbol(key) != null;
    }
    
    public void reset() {
        for (final AtomicLong value : this.opsCount.values()) {
            value.set(0L);
        }
    }
    
    static {
        LOG = S3AFileSystem.LOG;
    }
    
    private class LongIterator implements Iterator<StorageStatistics.LongStatistic>
    {
        private Iterator<Map.Entry<Statistic, AtomicLong>> iterator;
        
        private LongIterator() {
            this.iterator = Collections.unmodifiableSet((Set<? extends Map.Entry<Statistic, AtomicLong>>)S3AStorageStatistics.this.opsCount.entrySet()).iterator();
        }
        
        @Override
        public boolean hasNext() {
            return this.iterator.hasNext();
        }
        
        @Override
        public StorageStatistics.LongStatistic next() {
            if (!this.iterator.hasNext()) {
                throw new NoSuchElementException();
            }
            final Map.Entry<Statistic, AtomicLong> entry = this.iterator.next();
            return new StorageStatistics.LongStatistic(entry.getKey().getSymbol(), entry.getValue().get());
        }
        
        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
