package io.airlift.concurrent;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestBoundedExecutor
        extends AbstractTestBoundedExecutor
{
    private ExecutorService executorService;

    @BeforeAll
    public void setUp()
    {
        executorService = newCachedThreadPool();
    }

    @AfterAll
    public void tearDown()
    {
        executorService.shutdownNow();
    }

    @Override
    protected Executor getExecutor(int maxThreads)
    {
        return new BoundedExecutor(executorService, maxThreads);
    }

    @Test
    public void testExecutorCorruptionDetection()
    {
        AtomicBoolean reject = new AtomicBoolean();
        Executor executor = command -> {
            if (reject.get()) {
                throw new RejectedExecutionException("Reject for testing");
            }
            executorService.execute(command);
        };
        BoundedExecutor boundedExecutor = new BoundedExecutor(executor, 1); // Enforce single thread

        // Force the underlying executor to fail
        reject.set(true);
        assertThatThrownBy(() -> boundedExecutor.execute(() -> fail("Should not be run")))
                .isInstanceOf(RejectedExecutionException.class)
                .hasMessage("Reject for testing");

        // Recover the underlying executor, but all new tasks should fail
        reject.set(false);
        assertThatThrownBy(() -> boundedExecutor.execute(() -> fail("Should not be run")))
                .isInstanceOf(RejectedExecutionException.class)
                .hasMessage("BoundedExecutor is in a failed state");
    }
}
