package io.github.colinzhu.taskqueue.manager;

import io.vertx.core.Future;
import io.vertx.sqlclient.SqlConnection;

import java.time.Duration;

public interface TaskQueueManager {
    static TaskQueueManager taskQueue() {
        return TaskQueueManagerDbImpl.getInstance();
    }
    Future<?> enqueue(SqlConnection sqlConnection, String queueName, String refNumber, String payload, Duration processDelay);
    Future<?> success(SqlConnection sqlConnection, long taskId);
    Future<?> failure(SqlConnection sqlConnection, long taskId);
    Future<?> reenqueue(SqlConnection sqlConnection, long taskId, Duration delay);
}
