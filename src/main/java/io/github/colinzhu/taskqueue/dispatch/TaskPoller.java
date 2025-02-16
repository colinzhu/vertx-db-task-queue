package io.github.colinzhu.taskqueue.dispatch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.github.colinzhu.taskqueue.internal.TaskEntity;
import io.github.colinzhu.taskqueue.internal.TaskQueueMetrics;
import io.github.colinzhu.taskqueue.internal.TaskRepo;
import io.github.colinzhu.taskqueue.process.Task;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.SqlConnection;
import lombok.extern.slf4j.Slf4j;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static io.github.colinzhu.taskqueue.internal.TaskStatus.ERROR;
import static io.github.colinzhu.taskqueue.internal.TaskStatus.PROCESSING;

@Slf4j
public class TaskPoller<T> {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());
    private final Vertx vertx;
    private final JDBCPool pool;
    private final TaskPollerConfig<T> config;
    private final String pollerId;
    private final String pollerInstance;
    private final TaskQueueMetrics metrics = new TaskQueueMetrics();
    private boolean isToStop = false;
    private boolean isStopped = true; // default is stopped, until start() is invoked
    private long timerId = -1;
    private boolean isNoTaskWaitingForTimer = false;
    private boolean isWaiting = false;
    private final TaskPollerRepo taskPollerRepo = new TaskPollerRepo();
    private final TaskRepo taskRepo = new TaskRepo();

    public TaskPoller(Vertx vertx, JDBCPool pool, TaskPollerConfig<T> config) {
        this.vertx = vertx;
        this.pool = pool;
        this.config = config;
        this.pollerInstance = "poller-" +  UUID.randomUUID(); // add "poller-" prefix to solve oracle storing uuid issue
        this.pollerId = "poller-" + config.getQueueName() + "-" + pollerInstance;

        String eventBusAddress = "poller." + config.getQueueName();
        vertx.eventBus().consumer(eventBusAddress, message -> {
            // If waiting for a timer, cancel it and fetch tasks immediately
            if (isNoTaskWaitingForTimer) {
                vertx.cancelTimer(timerId);
                log.debug("{} new task notification received, taskId={}, timerId={} cancelled, start to fetch tasks. ", pollerId, message.body(), timerId);
                fetchBatchAndProcess();
            } else {
                log.debug("{} new task notification received, ignored, taskId={}, because it's not no task and waiting for the timer, isStopped={}", pollerId, message.body(), isStopped);
            }
        });

        log.info("{} created: {}", pollerId, config);
    }

    private static <T> Task<T> convertTaskEntityToTask(TaskEntity taskEntity, Class<T> payloadClass) {
        try {
            return new Task<>(
                    taskEntity.getId(),
                    taskEntity.getQueueName(),
                    taskEntity.getAttempt(),
                    OBJECT_MAPPER.readValue(taskEntity.getPayload(), payloadClass)
            );
        } catch (JsonProcessingException e) {
            throw new RuntimeException("failed to deserialize JSON string to object. JSON: " + taskEntity.getPayload(), e);
        }
    }

    private static String getStackTrace(Throwable t) {
        StringWriter sw = new StringWriter();
        t.printStackTrace(new PrintWriter(sw));
        return sw.toString();
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
                if (isWaiting) {
                    vertx.cancelTimer(timerId);
                    log.info("{} was waiting for next poll, timer cancelled, can stop immediately", pollerId);
                    isStopped = true;
                }
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
        isWaiting = false;
        if (isToStop) {
            log.info("{} isToStop=true, stop dispatch", pollerId);
            isStopped = true;
            return;
        }
        isStopped = false;
        long start = System.currentTimeMillis();
        String pollId = pollerId + "-" + start;
        fetchBatch2(pool, config.getQueueName(), config.getBatchSize(), config.getNextProcessDelay(), pollerInstance)
                .onSuccess(batch -> {
                    long end = System.currentTimeMillis();
                    recordTime("fetch.batch", start, "result", "success");
                    if (batch.isEmpty()) {
                        log.debug("{} tasks fetched, size=0. Time:{}ms. Fetch again in {}", pollId, end - start, config.getNoTaskPollInterval());
                        rerunWithDelayIfNecessary(config.getNoTaskPollInterval(), true);
                    } else {
                        processBatch(batch, pollId, start);
                    }
                }).onFailure(e -> {
                    recordTime("fetch.batch", start, "result", "failure");
                    log.error("{} failed to fetch tasks, retry in {}", pollId, config.getErrorCheckOutInterval(), e);
                    rerunWithDelayIfNecessary(config.getErrorCheckOutInterval());
                });
    }

    private void processBatch(List<TaskEntity> batch, String pollId, long fetchStart) {
        List<Long> taskIdList = batch.stream().map(TaskEntity::getId).toList();
        List<String> refNumberList = batch.stream().map(TaskEntity::getReferenceNumber).toList();
        String logTasks = "size=%d, taskIdList=%s, refList=%s".formatted(batch.size(), taskIdList, refNumberList);
        log.debug("{} tasks fetched, {} Time:{}ms", pollId, logTasks, System.currentTimeMillis() - fetchStart);

        long processStart = System.currentTimeMillis();
        List<Future<?>> futures = batch.stream().map(this::processSingleTask).collect(Collectors.toList());
        Future.join(futures).onSuccess(event -> {
            long end = System.currentTimeMillis();
            recordTime("process.batch", processStart, "result", "success");
            log.info("{} tasks processed, {}, Fetch and process time:{}ms, fetch time:{}ms, process time:{}ms", pollId, logTasks, end - fetchStart, processStart - fetchStart, end - processStart);
            rerunWithDelayIfNecessary(config.getHasTaskPollInterval());
        }).onFailure(e -> {
            // all task should be processed successfully or recovered (marked as ERROR), this is only a safety net e.g. not able to mark task as ERROR into DB
            recordTime("process.batch", processStart, "result", "failure");
            log.error("{} {}, at least one item failed (even unable to mark as ERROR).", pollId, logTasks, e);
            rerunWithDelayIfNecessary(config.getErrorProcessTasksInterval());
        });
    }

    private Future<?> processSingleTask(TaskEntity taskEntity) {
        long start = System.currentTimeMillis();
        return Future.succeededFuture()
                .map(res -> convertTaskEntityToTask(taskEntity, config.getPayloadClass()))
                .compose(tTask -> vertx.eventBus().request(taskEntity.getQueueName(), tTask, new DeliveryOptions().setSendTimeout(config.getTimeout().toMillis())))
                .onSuccess(res -> {
                    long end = System.currentTimeMillis();
                    recordTime("process.single", start, "result", "success");
                    log.info("{} task processed successfully, taskId={}, time={}ms, response={}", pollerId, taskEntity.getId(), end - start, res.body());
                })
                .recover(err -> {
                    log.error("{} error process task, taskId={}, will TRY to update task status to ERROR.", pollerId, taskEntity.getId(), err);
                    return markTaskAsError(taskEntity, start, err);
                });
    }

    private Future<Message<Object>> markTaskAsError(TaskEntity taskEntity, long start, Throwable err) {
        // for recover to mark the task as ERROR, it needs to be in a separate connection
        return pool.withConnection(conn -> fail(conn, taskEntity.getId(), getStackTrace(err))
                        .onSuccess(count -> log.info("{} updated task status to ERROR, taskId={}, time={}ms", pollerId, taskEntity.getId(), System.currentTimeMillis() - start))
                        .onFailure(e -> log.error("{} failed to update task status to ERROR, taskId={}", pollerId, taskEntity.getId(), e))
                        .onComplete(result -> recordTime("process.single", start,"result", result.succeeded() ? "success" : "failure")))
                .map(count -> null); // in order to convert Future<Integer> to align with eventbus.request's return type Future<Message<Object>>
    }

    private Future<Integer> fail(SqlConnection sqlConnection, long taskId, String processResult) {
        return taskRepo.updateStatusFromWithResult(sqlConnection, taskId, PROCESSING, ERROR, processResult);
    }

    private void rerunWithDelayIfNecessary(Duration delay) {
        rerunWithDelayIfNecessary(delay, false);
    }

    private void rerunWithDelayIfNecessary(Duration delay, boolean isNoTask) {
        if (isToStop) {
            log.info("{} isToStop=true, stop dispatch", pollerId);
            isStopped = true;
            return;
        }
        if (config.isPollNextBatch()) {
            if (Duration.ZERO.equals(delay)) {
                fetchBatchAndProcess();
            } else {
                log.debug("{} rerun delay={}", pollerId, delay);
                timerId = vertx.setTimer(delay.toMillis(), id -> fetchBatchAndProcess());
                isWaiting = true;
                if (isNoTask) { // only allow to cancel timer when it's normal and no task case
                    isNoTaskWaitingForTimer = true;
                }
            }
        } else {
            log.info("{} isPollNextBatch=false, no more dispatch", pollerId);
            isStopped = true;
        }
    }

    private void recordTime(String type, long startTime, String... tags) {
        metrics.recordTime("taskqueue.poller." + type, config.getQueueName(), startTime, tags);
    }

    private Future<List<TaskEntity>> fetchBatch(JDBCPool pool, String queueName, int batchSize, Duration nextProcessDelay) {
        return pool.withTransaction(sqlConnection -> taskPollerRepo.checkout(sqlConnection, queueName, batchSize, nextProcessDelay));
    }

    private Future<List<TaskEntity>> fetchBatch2(JDBCPool pool, String queueName, int batchSize, Duration nextProcessDelay, String pollerInstance) {
        return taskPollerRepo.checkout2(pool, queueName, batchSize, nextProcessDelay, pollerInstance);
    }

}
