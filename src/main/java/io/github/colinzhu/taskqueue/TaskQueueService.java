package io.github.colinzhu.taskqueue;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.SqlConnection;

import java.time.Duration;
import java.util.function.Function;

public interface TaskQueueService {
    static TaskQueueService taskQueue(JDBCPool pool) {
        return TaskQueueServiceDbImpl.getInstance(pool);
    }
    static TaskQueueService taskQueue(Vertx vertx, JDBCPool pool) {
        return TaskQueueServiceDbEventBusImpl.getInstance(vertx, pool);
    }
    default <T> Future<Task<String>> enqueue(SqlConnection sqlConnection, String queueName, String refNumber, T payload) {
        return enqueue(sqlConnection, queueName, refNumber, payload, Duration.ZERO);
    }
    <T> Future<T> withTaskQueueTxn(Function<SqlConnection, Future<T>> function, Function<SqlConnection, Future<Integer>> taskFunction);
    <T> Future<Task<String>> enqueue(SqlConnection sqlConnection, String queueName, String refNumber, T payload, Duration processDelay);
    Future<Integer> finish(SqlConnection sqlConnection, long taskId);
    Future<Integer> fail(SqlConnection sqlConnection, long taskId);
    Future<Integer> reenqueue(SqlConnection sqlConnection, long taskId, Duration delay);
}
