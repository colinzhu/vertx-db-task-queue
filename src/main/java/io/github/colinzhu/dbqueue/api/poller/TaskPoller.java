package io.github.colinzhu.dbqueue.api.poller;

import io.github.colinzhu.dbqueue.api.QueueConfig;
import io.github.colinzhu.dbqueue.api.Task;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.jdbcclient.JDBCPool;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Slf4j
@Accessors(fluent = true)
public class TaskPoller {
    private final Vertx vertx;
    private final QueueConfig queueConfig;
    private final Function<Task, Future<?>> taskProcessor;
    private final Supplier<Future<List<Task>>> taskSelector;
    @Setter
    private Duration noTaskPollInterval = Duration.ofSeconds(5);
    @Setter
    private Duration hasTaskPollInterval = Duration.ofMillis(1);
    @Setter
    private Duration processErrRetryInterval = Duration.ofSeconds(5);
    @Setter
    private Duration errPollingRetryInterval = Duration.ofSeconds(60);
    @Setter
    private boolean processNextBatch = true;

    public TaskPoller(Vertx vertx, JDBCPool pool, QueueConfig queueConfig) {
        this.vertx = vertx;
        this.queueConfig = queueConfig;
        this.taskProcessor = queueConfig.getTaskProcessor();
        this.taskSelector = new TaskSelector(pool, queueConfig);
    }

    public void start() {
        fetchBatchAndProcess();
    }

    /**
     * Frequently trigger the taskSelector to fetch tasks and then invoke the taskProcessor to process them
     */
    public void fetchBatchAndProcess() {
        long batchId = System.currentTimeMillis();
        taskSelector.get().onSuccess(batch -> {
            if (batch.size() > 0) {
                log.info("[{}][Batch:{}] size:{}, fetched. Time:{}ms", taskSelector, batchId, batch.size(), System.currentTimeMillis() - batchId);
                long procStart = System.currentTimeMillis();
                List<Future<?>> futures = batch.stream().map(taskProcessor).collect(Collectors.toList());
                Future.join(futures).onSuccess(event -> {
                    long end = System.currentTimeMillis();
                    log.info("[{}][Batch:{}] size:{}, all items succeeded. Fetch and process time:{}ms, fetch time:{}ms process time:{}ms", taskSelector, batchId, futures.size(), end - batchId, procStart - batchId, end - procStart);
                    if (processNextBatch) {
                        rerunWithDelay(hasTaskPollInterval);
                    }
                }).onFailure(e -> {
                    // item consumer should handle all exceptions, this is only a safety net e.g. not able to update record status in DB
                    log.error("[{}][Batch:{}] size:{}, all items completed, but at least one item failed. Retry in {}", taskSelector, batchId, batch.size(), processErrRetryInterval, e);
                    rerunWithDelay(processErrRetryInterval);
                });
            } else {
                if (processNextBatch) {
                    log.info("[{}][Batch:{}] size:0. Time:{}ms. Fetch again in {}", taskSelector, batchId, System.currentTimeMillis() - batchId, noTaskPollInterval);
                    rerunWithDelay(noTaskPollInterval);
                } else {
                    log.info("[{}][Batch:{}] size:0, no more fetching.", taskSelector, batchId);
                }
            }
        }).onFailure(e -> {
            log.error("[{}][Batch:{}] Failed to fetch batch, retry in {}", taskSelector, batchId, errPollingRetryInterval, e);
            rerunWithDelay(errPollingRetryInterval);
        });
    }

    private void rerunWithDelay(Duration delay) {
        vertx.setTimer(delay.toMillis(), id -> fetchBatchAndProcess());
    }

}
