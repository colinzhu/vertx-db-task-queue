package io.github.colinzhu.taskqueue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.SqlConnection;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public class TaskPoller<T> {
    private final Vertx vertx;
    private final JDBCPool pool;
    private final PollConfig<T> config;
    private final TaskEntityRepo taskEntityRepo;
    private final TaskQueueService taskQueueService;
    private final ObjectMapper objectMapper;
    private boolean isToStop = false;

    public TaskPoller(Vertx vertx, JDBCPool pool, PollConfig<T> config) {
        this(vertx, pool, config, TaskEntityRepo.getInstance(), TaskQueueService.taskQueue(),
                new ObjectMapper().registerModule(new JavaTimeModule()));
        log.info("Poller instance[{}] created: {}", this.hashCode(), config);
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
            log.info("isToStop=true, stop polling");
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
                    log.error("[{}] Failed to check out batch of tasks, retry in {}", pollId, config.getErrorCheckOutInterval(), e);
                    rerunWithDelayIfNecessary(config.getErrorCheckOutInterval());
                });
    }

    private Future<List<TaskEntity>> checkOutTasks(SqlConnection sqlConnection) {
        return taskEntityRepo.checkout(sqlConnection, config.getQueueName(), config.getBatchSize(), config.getNextProcessDelay());
    }

    private void handleFetchedTasks(List<TaskEntity> batch, String pollId, long start) {
        List<Long> taskIdList = batch.stream().map(TaskEntity::getId).collect(Collectors.toList());
        List<String> refNumberList = batch.stream().map(TaskEntity::getReferenceNumber).collect(Collectors.toList());
        String logTmpl = "[%s] size:%d, taskIdList:%s, refList:%s".formatted(pollId, batch.size(), taskIdList, refNumberList);
        log.debug("{} fetched. Time:{}ms", logTmpl, System.currentTimeMillis() - start);
        long processStart = System.currentTimeMillis();
        List<Future<Integer>> futures = batch.stream().map(this::processTask).collect(Collectors.toList());
        Future.join(futures).onSuccess(event -> {
            long end = System.currentTimeMillis();
            log.info("{}, all tasks finished or marked as ERROR. Fetch and process time:{}ms, fetch time:{}ms, process time:{}ms", logTmpl, end - start, processStart - start, end - processStart);
            rerunWithDelayIfNecessary(config.getHasTaskPollInterval());
        }).onFailure(e -> {
            // all task should be processed successfully or recovered (marked as ERROR), this is only a safety net e.g. not able to mark task as ERROR into DB
            log.error("{}, at least one item failed (even unable to mark as ERROR).", logTmpl, e);
            rerunWithDelayIfNecessary(config.getErrorProcessTasksInterval());
        });
    }

    private void rerunWithDelayIfNecessary(Duration delay) {
        if (isToStop) {
            log.info("isToStop=true, stop polling");
            return;
        }
        if (config.isPollNextBatch()) {
            vertx.setTimer(delay.toMillis(), id -> fetchBatchAndProcess());
        } else {
            log.info("[{}] isPollNextBatch=false, no more polling", config.getQueueName());
        }
    }

    private Task<T> convertTask(TaskEntity taskEntity) {
        try {
            return new Task<>(
                    taskEntity.getId(),
                    objectMapper.readValue(taskEntity.getPayload(), config.getPayloadClass())
            );
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to deserialize JSON string to object. JSON: " + taskEntity.getPayload(), e);
        }
    }

    private Future<Integer> processTask(TaskEntity taskEntity) {
        return Future.succeededFuture()
                .map(res -> convertTask(taskEntity))
                .compose(tTask -> config.getTaskProcessor().apply(tTask))
                .recover(err -> {
                    log.error("[{}][taskId:{}] Error when try to process the task. Mark it as ERROR.", config.getQueueName(), taskEntity.getId(), err);
                    // for recover to mark the task as ERROR, it needs to be in a separate connection
                    return pool.withConnection(conn -> ((TaskQueueServiceDbImpl)taskQueueService).fail(conn, taskEntity.getId()));
                });
    }

}
