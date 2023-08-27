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
class TaskQueueManagerDbImpl implements TaskQueueManager {
    private static final TaskQueueManager instance = new TaskQueueManagerDbImpl();

    public static TaskQueueManager getInstance() {
        return instance;
    }

    private final TaskDao taskDao = TaskDao.getInstance();
    private TaskQueueManagerDbImpl() {
    }

    public Future<?> enqueue(SqlConnection sqlConnection, String queueName, String refNumber, String payload, Duration processDelay) {
        return taskDao.insert(sqlConnection, queueName, refNumber, payload, processDelay);
    }

    @Override
    public Future<?> success(SqlConnection sqlConnection, long taskId) {
        return taskDao.delete(sqlConnection, taskId);
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
