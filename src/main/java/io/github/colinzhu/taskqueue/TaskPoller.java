package io.github.colinzhu.taskqueue;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.jdbcclient.JDBCPool;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class TaskPoller {
    private final Vertx vertx;
    private final PollConfig config;
    private final TaskRepo taskRepo;
    private final JDBCPool pool;
    private boolean isToStop = false;

    public TaskPoller(Vertx vertx, JDBCPool pool, PollConfig config) {
        this.vertx = vertx;
        this.config = config;
        this.pool = pool;
        this.taskRepo = TaskRepo.getInstance();
    }

    public void start() {
        isToStop = false;
        fetchBatchAndProcess();
    }
    public void stop() {
        isToStop = true;
    }

    /**
     * Frequently trigger the taskSelector to fetch tasks and then invoke the taskProcessor to process them
     */
    public void fetchBatchAndProcess() {
        if (isToStop) {
            log.info("isToRun=false, stop polling");
            return;
        }
        long start = System.currentTimeMillis();
        String pollId = "PollId:" + config.getQueueName() + "-" + this.hashCode() + "-" + start;
        pool.withTransaction(sqlConnection -> taskRepo.checkout(sqlConnection, config.getQueueName(), config.getBatchSize(), config.getNextProcessDelay()))
        .onSuccess(batch -> {
            if (batch.size() > 0) {
                log.debug("[{}] size:{}, fetched. Time:{}ms", pollId, batch.size(), System.currentTimeMillis() - start);
                long procStart = System.currentTimeMillis();
                List<Future<?>> futures = batch.stream().map(config.getTaskProcessor()).collect(Collectors.toList());
                Future.join(futures).onSuccess(event -> {
                    long end = System.currentTimeMillis();
                    log.info("[{}] size:{}, all items succeeded. Fetch and process time:{}ms, fetch time:{}ms, process time:{}ms", pollId, futures.size(), end - start, procStart - start, end - procStart);
                    if (config.isPollNextBatch()) {
                        rerunWithDelayIfNecessary(config.getHasTaskPollInterval());
                    }
                }).onFailure(e -> {
                    // item consumer should handle all exceptions, this is only a safety net e.g. not able to update record status in DB
                    log.error("[{}] size:{}, all items completed, but at least one item failed. Retry in {}", pollId, batch.size(), config.getProcessErrRetryInterval(), e);
                    rerunWithDelayIfNecessary(config.getProcessErrRetryInterval());
                });
            } else {
                log.debug("[{}] size:0. Time:{}ms. Fetch again in {}", pollId, System.currentTimeMillis() - start, config.getNoTaskPollInterval());
                rerunWithDelayIfNecessary(config.getNoTaskPollInterval());
            }
        }).onFailure(e -> {
            log.error("[{}] Failed to fetch batch, retry in {}", pollId, config.getErrPollingRetryInterval(), e);
            rerunWithDelayIfNecessary(config.getErrPollingRetryInterval());
        });
    }

    private void rerunWithDelayIfNecessary(Duration delay) {
        if (isToStop) {
            log.info("isToRun=false, stop polling");
            return;
        }
        if (config.isPollNextBatch()) {
            vertx.setTimer(delay.toMillis(), id -> fetchBatchAndProcess());
        } else {
            log.info("[{}] isPollNextBatch=false, no more polling", config.getQueueName());
        }
    }

}
