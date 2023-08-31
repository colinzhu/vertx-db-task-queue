package io.github.colinzhu.taskqueue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.SqlConnection;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class TaskPoller<T> {
    private final Vertx vertx;
    private final PollConfig<T> config;
    private final TaskRepo taskRepo;
    private final JDBCPool pool;
    private boolean isToStop = false;
    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    public TaskPoller(Vertx vertx, JDBCPool pool, PollConfig<T> config) {
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
    private void fetchBatchAndProcess() {
        if (isToStop) {
            log.info("isToRun=false, stop polling");
            return;
        }
        long start = System.currentTimeMillis();
        String pollId = "PollId:" + config.getQueueName() + "-" + this.hashCode() + "-" + start;
        pool.withTransaction(this::checkOutTasks)
                .onSuccess(batch -> {
                    if (batch.isEmpty()) {
                        log.debug("[{}] size:0. Time:{}ms. Fetch again in {}", pollId, System.currentTimeMillis() - start, config.getNoTaskPollInterval());
                        rerunWithDelayIfNecessary(config.getNoTaskPollInterval());
                    } else {
                        handleFetchedTasks(batch, pollId, start);
                    }
                }).onFailure(e -> {
                    log.error("[{}] Failed to check out batch of tasks, retry in {}", pollId, config.getErrPollingRetryInterval(), e);
                    rerunWithDelayIfNecessary(config.getErrPollingRetryInterval());
                });
    }

    private Future<List<Task<String>>> checkOutTasks(SqlConnection sqlConnection) {
        return taskRepo.checkout(sqlConnection, config.getQueueName(), config.getBatchSize(), config.getNextProcessDelay());
    }

    private void handleFetchedTasks(List<Task<String>> batch, String pollId, long start) {
        log.debug("[{}] size:{}, fetched. Time:{}ms", pollId, batch.size(), System.currentTimeMillis() - start);
        long processStart = System.currentTimeMillis();
        List<Future<Integer>> futures = batch.stream().map(this::processTask).collect(Collectors.toList());
        Future.join(futures).onSuccess(event -> {
            long end = System.currentTimeMillis();
            log.info("[{}] size:{}, all tasks processing completed (finished or marked as ERROR). Fetch and process time:{}ms, fetch time:{}ms, process time:{}ms", pollId, futures.size(), end - start, processStart - start, end - processStart);
            rerunWithDelayIfNecessary(config.getHasTaskPollInterval());
        }).onFailure(e -> {
            // all task should be processed successfully or recovered (marked as ERROR), this is only a safety net e.g. not able to mark task as ERROR into DB
            log.error("[{}] size:{}, at least one item failed (even unable to mark as ERROR). Retry in {}", pollId, batch.size(), config.getProcessErrRetryInterval(), e);
            rerunWithDelayIfNecessary(config.getProcessErrRetryInterval());
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

    private Task<T> convertTask(Task<String> stringTask) {
        try {
            return new Task<>(stringTask.getId(), objectMapper.readValue(stringTask.getPayload(), config.getPayloadClass()));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to deserialize JSON string to object. JSON: " + stringTask.getPayload(), e);
        }
    }

    private Future<Integer> processTask(Task<String> stringTask) {
        return Future.succeededFuture()
                .map(res -> convertTask(stringTask))
                .compose(tTask -> config.getTaskProcessor().apply(tTask))
                .recover(err -> {
                    log.error("[{}][taskId:{}] Error when try to process the task.", config.getQueueName(), stringTask.getId(), err);
                    // for recover to mark the task as ERROR, it needs to be in a separate connection
                    return pool.withConnection(conn -> TaskQueueService.taskQueue().fail(conn, stringTask.getId()));
                });
    }

}
