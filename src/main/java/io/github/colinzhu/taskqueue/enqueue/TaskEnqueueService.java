package io.github.colinzhu.taskqueue.enqueue;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.sqlclient.SqlConnection;

import java.time.Duration;

/**
 * <pre>
 * Task enqueue Service to:
 * enqueue - put a task into a queue
 * </pre>
 */
public interface TaskEnqueueService {
    static TaskEnqueueService taskQueue(Vertx vertx) {
        return new TaskEnqueueServiceImpl(vertx);
    }

    /**
     * Enqueues a task into the task queue with no delay time.
     * @param sqlConnection DB transaction
     * @param queueName     queue name
     * @param refNumber     reference number of payload
     * @param payload       payload object which will be marshalled to json string
     * @param <T>           task payload type
     * @return future of a task id
     */
    <T> Future<Long> enqueue(SqlConnection sqlConnection, String queueName, String refNumber, T payload);

    /**
     * Enqueues a task into the task queue with a delay time.
     * @param sqlConnection DB transaction
     * @param queueName     queue name
     * @param refNumber     reference number of payload
     * @param payload       payload object which will be marshalled to json string
     * @param delay         process delay time after putting into the queue
     * @param <T>           task payload type
     * @return future of a task which has been stored into task queue
     */
    <T> Future<Long> enqueue(SqlConnection sqlConnection, String queueName, String refNumber, T payload, Duration delay);
}
