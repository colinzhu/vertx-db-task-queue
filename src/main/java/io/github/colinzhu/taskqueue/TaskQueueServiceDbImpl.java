package io.github.colinzhu.taskqueue;

import io.vertx.core.Future;
import io.vertx.sqlclient.SqlConnection;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;

/**
 * QueueClient
 * task enqueue into queue
 * task dispatch to handler
 * task dequeue from queue (success, failure, reprocess)
 * <p>
 * Client put task into -> Message Broker dispatch -> Client dequeue task
 */
@Slf4j
class TaskQueueServiceDbImpl implements TaskQueueService {
    private static final TaskQueueService instance = new TaskQueueServiceDbImpl();

    public static TaskQueueService getInstance() {
        return instance;
    }

    private final TaskRepo taskRepo = TaskRepo.getInstance();
    private TaskQueueServiceDbImpl() {
    }

    public Future<?> enqueue(SqlConnection sqlConnection, String queueName, String refNumber, String payload, Duration processDelay) {
        return taskRepo.insert(sqlConnection, queueName, refNumber, payload, processDelay);
    }

    @Override
    public Future<?> success(SqlConnection sqlConnection, long taskId) {
        return taskRepo.delete(sqlConnection, taskId);
    }

    @Override
    public Future<?> failure(SqlConnection sqlConnection, long taskId) {
        return Future.succeededFuture();
    }

    @Override
    public Future<?> reenqueue(SqlConnection sqlConnection, long taskId, Duration delay) {
        return Future.succeededFuture();
    }
}
