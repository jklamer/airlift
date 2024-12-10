package io.airlift.concurrent;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly;
import static org.assertj.core.api.Assertions.assertThat;

public class TestBoundedVirtualThreadExecutorService
        extends AbstractTestBoundedExecutor
{
    @Override
    protected Executor getExecutor(int maxThreads)
    {
        return new BoundedVirtualThreadExecutorService(maxThreads);
    }

    @Test
    public void testExecutorServiceShutdown()
    {
        int maxConcurrency = 10;
        int taskCount = 10_000;
        ExecutorService service = new BoundedVirtualThreadExecutorService(maxConcurrency);
        CountDownLatch tasksCanExit = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(taskCount);
        CountDownLatch initLatch = new CountDownLatch(maxConcurrency);
        AtomicInteger count = new AtomicInteger();
        AtomicBoolean failed = new AtomicBoolean(false);
        for (int i = 0; i < taskCount; i++) {
            service.execute(() -> {
                count.incrementAndGet();
                initLatch.countDown();
                doneLatch.countDown();
                try {
                    tasksCanExit.await();
                }
                catch (InterruptedException e) {
                    failed.set(true);
                }
            });
        }
        awaitUninterruptibly(initLatch);
        service.shutdown();
        assertThat(count.get()).isEqualTo(maxConcurrency);
        tasksCanExit.countDown();
        assertThat(awaitUninterruptibly(doneLatch, 1, TimeUnit.MINUTES)).isTrue(); // Wait for tasks to complete
        assertThat(failed.get()).isFalse();
    }

    @Test
    public void testExecutorServiceShutdownNow()
            throws InterruptedException
    {
        int maxConcurrency = 10;
        int taskCount = 10_000;
        ExecutorService service = new BoundedVirtualThreadExecutorService(maxConcurrency);
        CountDownLatch initLatch = new CountDownLatch(maxConcurrency);
        CountDownLatch permaBlock = new CountDownLatch(1);
        AtomicInteger count = new AtomicInteger();
        for (int i = 0; i < taskCount; i++) {
            service.execute(() -> {
                count.incrementAndGet();
                initLatch.countDown();
                try {
                    permaBlock.await();
                }
                catch (InterruptedException e) {
                    // expected
                    return;
                }
            });
        }
        awaitUninterruptibly(initLatch);
        service.shutdownNow();
        Thread.sleep(100);
        assertThat(count.get()).isEqualTo(maxConcurrency);
    }
}
