package io.github.colinzhu.taskqueue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.vertx.core.Future;
import io.vertx.core.Promise;
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
    private final TaskQueueServiceImpl taskQueueServiceImpl;
    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    private String pollerId;
    private boolean isToStop = false;
    private boolean isStopped = true; // default is stopped, until start() is invoked
    private long timerId = -1;
    private boolean isNoTaskWaitingForTimer = false;
    public TaskPoller(Vertx vertx, JDBCPool pool, PollConfig<T> config) {
        this(vertx, pool, config, TaskEntityRepo.getInstance(), new TaskQueueServiceImpl(vertx, TaskEntityRepo.getInstance()));
        pollerId = "poller-" + config.getQueueName() + "-" + Integer.toHexString(this.hashCode());
        log.info("{} created: {}", pollerId, config);

        String eventBusAddress = "poller." + config.getQueueName();
        vertx.eventBus().consumer(eventBusAddress, message -> {
            // If waiting for a timer, cancel it and fetch tasks immediately
            if (isNoTaskWaitingForTimer) {
                vertx.cancelTimer(timerId);
                //waitingForTimer = false;
                log.debug("{} New task event received, timerId={} cancelled, start to process new tasks. taskId={}", pollerId, timerId, message.body());
                fetchBatchAndProcess();
            } else {
                log.debug("{} New task event received, ignored, because it's not no task and waiting for the timer. taskId={}, isStopped={}", pollerId, message.body(), isStopped);
            }
        });
    }

    public void start() {
        isToStop = false;
        isStopped = false;
        fetchBatchAndProcess();
    }

    public Future<Void> stop() {
        isToStop = true;
        log.info("{} stop triggered", pollerId);
        Promise<Void> promise = Promise.promise();
        if (isStopped) {
            log.info("{} stopped", pollerId);
            promise.complete();
        } else {
            vertx.setPeriodic(1000, id -> {
                log.info("{} stopping, checking status", pollerId);
                if (isStopped) {
                    log.info("{} stopped", pollerId);
                    promise.complete();
                    vertx.cancelTimer(id);
                }
            });
        }
        return promise.future();
    }

    /**
     * Frequently trigger the taskSelector to fetch tasks and then invoke the taskProcessor to process them
     */
    private void fetchBatchAndProcess() {
        isNoTaskWaitingForTimer = false;
        if (isToStop) {
            log.info("{} isToStop=true, stop polling", pollerId);
            isStopped = true;
            return;
        }
        isStopped = false;
        long start = System.currentTimeMillis();
        String pollId = pollerId + "-" + start;
        pool.withTransaction(this::checkOutTasks)
                .onSuccess(batch -> {
                    if (batch.isEmpty()) {
                        log.debug("{} size:0. Time:{}ms. Fetch again in {}", pollId, System.currentTimeMillis() - start, config.getNoTaskPollInterval());
                        rerunWithDelayIfNecessary(config.getNoTaskPollInterval(), true);
                    } else {
                        handleFetchedTasks(batch, pollId, start);
                    }
                }).onFailure(e -> {
                    log.error("{} Failed to check out batch of tasks, retry in {}", pollId, config.getErrorCheckOutInterval(), e);
                    rerunWithDelayIfNecessary(config.getErrorCheckOutInterval());
                });
    }

    private Future<List<TaskEntity>> checkOutTasks(SqlConnection sqlConnection) {
        return taskEntityRepo.checkout(sqlConnection, config.getQueueName(), config.getBatchSize(), config.getNextProcessDelay());
    }

    private void handleFetchedTasks(List<TaskEntity> batch, String pollId, long start) {
        List<Long> taskIdList = batch.stream().map(TaskEntity::getId).collect(Collectors.toList());
        List<String> refNumberList = batch.stream().map(TaskEntity::getReferenceNumber).collect(Collectors.toList());
        String logTmpl = "%s size:%d, taskIdList:%s, refList:%s".formatted(pollId, batch.size(), taskIdList, refNumberList);
        log.debug("{} fetched. Time:{}ms", logTmpl, System.currentTimeMillis() - start);
        long processStart = System.currentTimeMillis();
        List<Future<?>> futures = batch.stream().map(this::processTask).collect(Collectors.toList());
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
        rerunWithDelayIfNecessary(delay, false);
    }

    private void rerunWithDelayIfNecessary(Duration delay, boolean isNoTask) {
        if (isToStop) {
            log.info("{} isToStop=true, stop polling", pollerId);
            isStopped = true;
            return;
        }
        if (config.isPollNextBatch()) {
            if (Duration.ZERO.equals(delay)) {
                fetchBatchAndProcess();
            } else {
                log.debug("{} rerun delay={}", pollerId, delay);
                timerId = vertx.setTimer(delay.toMillis(), id -> fetchBatchAndProcess());
                if (isNoTask) { // only allow to cancel timer when it's normal and no task case
                    isNoTaskWaitingForTimer = true;
                }
            }
        } else {
            log.info("{} isPollNextBatch=false, no more polling", pollerId);
            isStopped = true;
        }
    }

    private Task<T> convertTask(TaskEntity taskEntity) {
        try {
            return new Task<>(
                    taskEntity.getId(),
                    taskEntity.getAttempt() + 1,
                    objectMapper.readValue(taskEntity.getPayload(), config.getPayloadClass())
            );
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to deserialize JSON string to object. JSON: " + taskEntity.getPayload(), e);
        }
    }

    private Future<?> processTask(TaskEntity taskEntity) {
        return Future.succeededFuture()
                .map(res -> convertTask(taskEntity))
                .compose(tTask -> config.getTaskProcessor().apply(tTask))
                .recover(err -> {
                    log.error("{} [taskId:{}] Error when try to process the task. Will try to update task status to ERROR.", pollerId, taskEntity.getId(), err);
                    // for recover to mark the task as ERROR, it needs to be in a separate connection
                    return pool.withConnection(conn -> taskQueueServiceImpl.fail(conn, taskEntity.getId()).compose(count -> Future.succeededFuture()));
                });
    }

}
