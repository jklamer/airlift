package io.airlift.concurrent;

import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor;

/**
 * Maintains the max concurrency of an executor service created using {@code java.util.concurrent.Executors#newFixedThreadPool(int)}
 * Useful for maintaining a bound on resource heavy threads while still taking advantage of virtual thread scheduling
 *
 * Use with Java 24
 */
public class BoundedVirtualThreadExecutorService
        extends AbstractExecutorService
{
    private final Semaphore executionPermits;
    private final ExecutorService delegate;

    public BoundedVirtualThreadExecutorService(int maxConcurrency)
    {
        checkArgument(maxConcurrency > 0, "maxConcurrency must be greater than 0");
        executionPermits = new Semaphore(maxConcurrency, true);
        delegate = newVirtualThreadPerTaskExecutor();
    }

    @Override
    public void execute(Runnable command)
    {
        delegate.execute(() -> {
            try {
                executionPermits.acquire();
            }
            catch (InterruptedException e) {
                // Known thread shutdown condition. Return without executing
                // interrupt the current virtual thread
                Thread.currentThread().interrupt();
                return;
            }
            try {
                command.run();
            }
            finally {
                executionPermits.release();
            }
        });
    }

    @Override
    public void shutdown()
    {
        delegate.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow()
    {
        return delegate.shutdownNow();
    }

    @Override
    public boolean isShutdown()
    {
        return delegate.isShutdown();
    }

    @Override
    public boolean isTerminated()
    {
        return delegate.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit)
            throws InterruptedException
    {
        return delegate.awaitTermination(timeout, unit);
    }
}
