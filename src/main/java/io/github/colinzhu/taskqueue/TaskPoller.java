package io.github.colinzhu.taskqueue;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.jdbcclient.JDBCPool;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public class TaskPoller<T> {
    private final Vertx vertx;
    private final JDBCPool pool;
    @Getter
    private final TaskPollerConfig<T> config;
    private final TaskQueueRepo taskQueueRepo;
    private final TaskQueueServiceImpl taskQueueServiceImpl;
    private final TaskQueueMetrics metrics = new TaskQueueMetrics();

    private String pollerId;
    private String pollerInstance;
    private boolean isToStop = false;
    private boolean isStopped = true; // default is stopped, until start() is invoked
    private long timerId = -1;
    private boolean isNoTaskWaitingForTimer = false;
    private boolean isWaiting = false;

    public TaskPoller(Vertx vertx, JDBCPool pool, TaskPollerConfig<T> config) {
        this(vertx, pool, config, TaskQueueRepo.getInstance(), new TaskQueueServiceImpl(vertx, TaskQueueRepo.getInstance()));
        pollerInstance = UUID.randomUUID().toString();
        pollerId = "poller-" + config.getQueueName() + "-" + pollerInstance;

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
            log.info("{} isToStop=true, stop polling", pollerId);
            isStopped = true;
            return;
        }
        isStopped = false;
        long start = System.currentTimeMillis();
        String pollId = pollerId + "-" + start;
        fetchBatch2()
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

    private Future<List<TaskEntity>> fetchBatch() {
        return pool.withTransaction(sqlConnection -> taskQueueRepo.checkout(sqlConnection, config.getQueueName(), config.getBatchSize(), config.getNextProcessDelay()));
    }

    private Future<List<TaskEntity>> fetchBatch2() {
        // 'update' and 'select' are in 2 different connections, not in one transaction
        var step1updateCount = pool.withConnection(sqlConnection -> taskQueueRepo.checkout2step1update(sqlConnection, config.getQueueName(), config.getBatchSize(), config.getNextProcessDelay(), pollerInstance));
        return step1updateCount
                .compose(count -> {
                    if (count > 0) {
                        return pool.withConnection(sqlConnection -> taskQueueRepo.checkout2step2select(sqlConnection, config.getQueueName(), pollerInstance));
                    } else {
                        return Future.succeededFuture(new ArrayList<>());
                    }
                }); // the caller has error log, so doesn't print error log here
    }

    private void processBatch(List<TaskEntity> batch, String pollId, long fetchStart) {
        List<Long> taskIdList = batch.stream().map(TaskEntity::getId).collect(Collectors.toList());
        List<String> refNumberList = batch.stream().map(TaskEntity::getReferenceNumber).collect(Collectors.toList());
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
                .map(res -> TaskQueueUtils.convertTaskEntityToTask(taskEntity, config.getPayloadClass()))
                .compose(tTask -> vertx.eventBus().request(taskEntity.getQueueName(), tTask, new DeliveryOptions().setSendTimeout(config.getTimeout().toMillis())))
                .onSuccess(res -> {
                    long end = System.currentTimeMillis();
                    recordTime("process.single", start, "result", "success");
                    log.info("{} task processed successfully, taskId={}, time={}ms, response={}", pollerId, taskEntity.getId(), end - start, res.body());
                })
                .recover(err -> {
                    log.error("{} error processing task, taskId={}, will TRY to update task status to ERROR.", pollerId, taskEntity.getId(), err);
                    // for recover to mark the task as ERROR, it needs to be in a separate connection
                    return pool.withConnection(conn -> taskQueueServiceImpl.fail(conn, taskEntity.getId(), TaskQueueUtils.getStackTrace(err))
                            .onFailure(e -> {
                                log.error("{} failed to update task status to ERROR, taskId={}", pollerId, taskEntity.getId(), e);
                                recordTime("process.single", start, "result", "recover-failed");
                            })
                            .compose(count -> {
                                recordTime("process.single", start, "result", "recovered");
                                return Future.succeededFuture();
                            }));
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
                isWaiting = true;
                if (isNoTask) { // only allow to cancel timer when it's normal and no task case
                    isNoTaskWaitingForTimer = true;
                }
            }
        } else {
            log.info("{} isPollNextBatch=false, no more polling", pollerId);
            isStopped = true;
        }
    }

    private void recordTime(String type, long startTime, String... tags) {
        metrics.recordTime("taskqueue.poller." + type, config.getQueueName(), startTime, tags);
    }
}
