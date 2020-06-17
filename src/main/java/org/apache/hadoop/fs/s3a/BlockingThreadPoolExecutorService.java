// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3a;

import org.slf4j.LoggerFactory;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.apache.hadoop.classification.InterfaceAudience;

@InterfaceAudience.Private
final class BlockingThreadPoolExecutorService extends SemaphoredDelegatingExecutor
{
    private static final Logger LOG;
    private static final AtomicInteger POOLNUMBER;
    private final ThreadPoolExecutor eventProcessingExecutor;
    
    static ThreadFactory getNamedThreadFactory(final String prefix) {
        final SecurityManager s = System.getSecurityManager();
        final ThreadGroup threadGroup = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
        return new ThreadFactory() {
            private final AtomicInteger threadNumber = new AtomicInteger(1);
            private final int poolNum = BlockingThreadPoolExecutorService.POOLNUMBER.getAndIncrement();
            private final ThreadGroup group = threadGroup;
            
            @Override
            public Thread newThread(final Runnable r) {
                final String name = prefix + "-pool" + this.poolNum + "-t" + this.threadNumber.getAndIncrement();
                return new Thread(this.group, r, name);
            }
        };
    }
    
    static ThreadFactory newDaemonThreadFactory(final String prefix) {
        final ThreadFactory namedFactory = getNamedThreadFactory(prefix);
        return new ThreadFactory() {
            @Override
            public Thread newThread(final Runnable r) {
                final Thread t = namedFactory.newThread(r);
                if (!t.isDaemon()) {
                    t.setDaemon(true);
                }
                if (t.getPriority() != 5) {
                    t.setPriority(5);
                }
                return t;
            }
        };
    }
    
    private BlockingThreadPoolExecutorService(final int permitCount, final ThreadPoolExecutor eventProcessingExecutor) {
        super(MoreExecutors.listeningDecorator((ExecutorService)eventProcessingExecutor), permitCount, false);
        this.eventProcessingExecutor = eventProcessingExecutor;
    }
    
    public static BlockingThreadPoolExecutorService newInstance(final int activeTasks, final int waitingTasks, final long keepAliveTime, final TimeUnit unit, final String prefixName) {
        final BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<Runnable>(waitingTasks + activeTasks);
        final ThreadPoolExecutor eventProcessingExecutor = new ThreadPoolExecutor(activeTasks, activeTasks, keepAliveTime, unit, workQueue, newDaemonThreadFactory(prefixName), new RejectedExecutionHandler() {
            @Override
            public void rejectedExecution(final Runnable r, final ThreadPoolExecutor executor) {
                BlockingThreadPoolExecutorService.LOG.error("Could not submit task to executor {}", (Object)executor.toString());
            }
        });
        eventProcessingExecutor.allowCoreThreadTimeOut(true);
        return new BlockingThreadPoolExecutorService(waitingTasks + activeTasks, eventProcessingExecutor);
    }
    
    int getActiveCount() {
        return this.eventProcessingExecutor.getActiveCount();
    }
    
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("BlockingThreadPoolExecutorService{");
        sb.append(super.toString());
        sb.append(", activeCount=").append(this.getActiveCount());
        sb.append('}');
        return sb.toString();
    }
    
    static {
        LOG = LoggerFactory.getLogger((Class)BlockingThreadPoolExecutorService.class);
        POOLNUMBER = new AtomicInteger(1);
    }
}
