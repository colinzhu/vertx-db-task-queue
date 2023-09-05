package io.github.colinzhu.taskqueue;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.sqlclient.SqlConnection;

import java.time.Duration;
import java.util.function.Function;

public interface TaskQueueService {
    static TaskQueueService taskQueue() {
        return TaskQueueServiceDbImpl.getInstance();
    }
    static TaskQueueService taskQueue(Vertx vertx) {
        return TaskQueueServiceDbEventBusImpl.getInstance(vertx);
    }
    default <T> Future<Task<T>> enqueue(SqlConnection sqlConnection, String queueName, String refNumber, T payload) {
        return enqueue(sqlConnection, queueName, refNumber, payload, Duration.ZERO);
    }
    default Function<SqlConnection, Future<Integer>> finish(Function<SqlConnection, Future<?>> function, long taskId) {
        return sqlConnection -> function.apply(sqlConnection)
                .compose(result -> finish(sqlConnection, taskId));
    }
    default Function<SqlConnection, Future<Integer>> reenqueue(Function<SqlConnection, Future<?>> function, long taskId, Duration delay) {
        return sqlConnection -> function.apply(sqlConnection)
                .compose(result -> reenqueue(sqlConnection, taskId, delay));
    }
    <T> Future<Task<T>> enqueue(SqlConnection sqlConnection, String queueName, String refNumber, T payload, Duration processDelay);
    Future<Integer> finish(SqlConnection sqlConnection, long taskId);
    Future<Integer> fail(SqlConnection sqlConnection, long taskId);
    Future<Integer> reenqueue(SqlConnection sqlConnection, long taskId, Duration delay);
}
