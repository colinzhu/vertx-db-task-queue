package io.github.colinzhu.dbqueue.api.impl;

import io.github.colinzhu.dbqueue.api.Task;
import io.github.colinzhu.dbqueue.api.TaskPoller;
import io.github.colinzhu.dbqueue.api.TaskProcessResult;
import io.github.colinzhu.dbqueue.internal.TaskRecord;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.Tuple;
import io.vertx.sqlclient.templates.SqlTemplate;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.List.of;

@Slf4j
@RequiredArgsConstructor
@Accessors(fluent = true)
public class TaskPollerImpl implements TaskPoller {
    private final Vertx vertx;
//    private final String queueName;
    private final Supplier<Future<List<TaskRecord>>> taskBatchSupplier;
    private final Function<Task, Future<TaskProcessResult>> taskProcessor;
    private int batchSize = 5;
    private Duration nextProcessDelay = Duration.ofMinutes(15);
    @Setter
    private int noTaskPollInterval = 5000;
    @Setter
    private int hasTaskPollInterval = 1;
    @Setter
    private int processErrRetryInterval = 5000;
    @Setter
    private int errPollingRetryInterval = 60 * 1000;
    @Setter
    private boolean processNextBatch = true;

    @Override
    public void start() {
        fetchBatchAndProcess();
    }

    public void fetchBatchAndProcess() {
        long batchId = System.currentTimeMillis();
        taskBatchSupplier.get().onSuccess(batch -> {
            if (batch.size() > 0) {
                log.info("[{}][Batch:{}] size:{}, fetched. Time:{}ms", null, batchId, batch.size(), System.currentTimeMillis() - batchId);
                long procStart = System.currentTimeMillis();
                List<Future<TaskProcessResult>> futures = batch.stream().map(record -> new Task(record.getId(), record.getPayload())).map(taskProcessor).collect(Collectors.toList());
                Future.join(futures).onSuccess(event -> {
                    long end = System.currentTimeMillis();
                    log.info("[{}][Batch:{}] size:{}, all items succeeded. Fetch and process time:{}ms, fetch time:{}ms process time:{}ms", null, batchId, futures.size(), end - batchId, procStart - batchId, end - procStart);
                    if (processNextBatch) {
                        rerunWithDelay(hasTaskPollInterval);
                    }
                }).onFailure(e -> {
                    // item consumer should handle all exceptions, this is only a safety net e.g. not able to update record status in DB
//                    log.error("[{}][Batch:{}] size:{}, all items completed, but at least one item failed. Retry in {}ms", queueName, batchId, batch.size(), processErrRetryInterval, e);
                    rerunWithDelay(processErrRetryInterval);
                });
            } else {
                if (processNextBatch) {
//                    log.debug("[{}][Batch:{}] size:0. Time:{}ms. Fetch again in {}ms", queueName, batchId, System.currentTimeMillis() - batchId, noTaskPollInterval);
                    rerunWithDelay(noTaskPollInterval);
                } else {
//                    log.info("[{}][Batch:{}] size:0, no more fetching.", queueName, batchId);
                }
            }
        }).onFailure(e -> {
//            log.error("[{}][Batch:{}] Failed to fetch batch, retry in {}ms", queueName, batchId, errPollingRetryInterval, e);
            rerunWithDelay(errPollingRetryInterval);
        });
    }

    private void rerunWithDelay(long delay) {
        vertx.setTimer(delay, id -> fetchBatchAndProcess());
    }

}
