package io.github.colinzhu.taskqueue;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.sqlclient.SqlConnection;

import java.time.Duration;

public interface TaskQueueService {
    static TaskQueueService taskQueue() {
        return TaskQueueServiceDbImpl.getInstance();
    }
    static TaskQueueService taskQueue(Vertx vertx) {
        return TaskQueueServiceDbEventBusImpl.getInstance(vertx);
    }
    <T> Future<?> enqueue(SqlConnection sqlConnection, String queueName, String refNumber, T payload, Duration processDelay);
    Future<Integer> finish(SqlConnection sqlConnection, long taskId);
    Future<Integer> fail(SqlConnection sqlConnection, long taskId);
    Future<Integer> reenqueue(SqlConnection sqlConnection, long taskId, Duration delay);
}
