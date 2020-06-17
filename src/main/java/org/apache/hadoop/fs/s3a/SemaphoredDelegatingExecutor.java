// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.hadoop.fs.s3a;

import java.util.concurrent.ExecutorService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Future;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.Collection;
import com.google.common.util.concurrent.ListeningExecutorService;
import java.util.concurrent.Semaphore;
import org.apache.hadoop.classification.InterfaceAudience;
import com.google.common.util.concurrent.ForwardingListeningExecutorService;

@InterfaceAudience.Private
class SemaphoredDelegatingExecutor extends ForwardingListeningExecutorService
{
    private final Semaphore queueingPermits;
    private final ListeningExecutorService executorDelegatee;
    private final int permitCount;
    
    SemaphoredDelegatingExecutor(final ListeningExecutorService executorDelegatee, final int permitCount, final boolean fair) {
        this.permitCount = permitCount;
        this.queueingPermits = new Semaphore(permitCount, fair);
        this.executorDelegatee = executorDelegatee;
    }
    
    protected ListeningExecutorService delegate() {
        return this.executorDelegatee;
    }
    
    public <T> List<Future<T>> invokeAll(final Collection<? extends Callable<T>> tasks) throws InterruptedException {
        throw new RuntimeException("Not implemented");
    }
    
    public <T> List<Future<T>> invokeAll(final Collection<? extends Callable<T>> tasks, final long timeout, final TimeUnit unit) throws InterruptedException {
        throw new RuntimeException("Not implemented");
    }
    
    public <T> T invokeAny(final Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        throw new RuntimeException("Not implemented");
    }
    
    public <T> T invokeAny(final Collection<? extends Callable<T>> tasks, final long timeout, final TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        throw new RuntimeException("Not implemented");
    }
    
    public <T> ListenableFuture<T> submit(final Callable<T> task) {
        try {
            this.queueingPermits.acquire();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return (ListenableFuture<T>)Futures.immediateFailedCheckedFuture((Exception)e);
        }
        return (ListenableFuture<T>)super.submit((Callable)new CallableWithPermitRelease(task));
    }
    
    public <T> ListenableFuture<T> submit(final Runnable task, final T result) {
        try {
            this.queueingPermits.acquire();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return (ListenableFuture<T>)Futures.immediateFailedCheckedFuture((Exception)e);
        }
        return (ListenableFuture<T>)super.submit((Runnable)new RunnableWithPermitRelease(task), (Object)result);
    }
    
    public ListenableFuture<?> submit(final Runnable task) {
        try {
            this.queueingPermits.acquire();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return (ListenableFuture<?>)Futures.immediateFailedCheckedFuture((Exception)e);
        }
        return (ListenableFuture<?>)super.submit((Runnable)new RunnableWithPermitRelease(task));
    }
    
    public void execute(final Runnable command) {
        try {
            this.queueingPermits.acquire();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        super.execute((Runnable)new RunnableWithPermitRelease(command));
    }
    
    public int getAvailablePermits() {
        return this.queueingPermits.availablePermits();
    }
    
    public int getWaitingCount() {
        return this.queueingPermits.getQueueLength();
    }
    
    public int getPermitCount() {
        return this.permitCount;
    }
    
    public String toString() {
        final StringBuilder sb = new StringBuilder("SemaphoredDelegatingExecutor{");
        sb.append("permitCount=").append(this.getPermitCount());
        sb.append(", available=").append(this.getAvailablePermits());
        sb.append(", waiting=").append(this.getWaitingCount());
        sb.append('}');
        return sb.toString();
    }
    
    class RunnableWithPermitRelease implements Runnable
    {
        private Runnable delegatee;
        
        public RunnableWithPermitRelease(final Runnable delegatee) {
            this.delegatee = delegatee;
        }
        
        @Override
        public void run() {
            try {
                this.delegatee.run();
            }
            finally {
                SemaphoredDelegatingExecutor.this.queueingPermits.release();
            }
        }
    }
    
    class CallableWithPermitRelease<T> implements Callable<T>
    {
        private Callable<T> delegatee;
        
        public CallableWithPermitRelease(final Callable<T> delegatee) {
            this.delegatee = delegatee;
        }
        
        @Override
        public T call() throws Exception {
            try {
                return this.delegatee.call();
            }
            finally {
                SemaphoredDelegatingExecutor.this.queueingPermits.release();
            }
        }
    }
}
