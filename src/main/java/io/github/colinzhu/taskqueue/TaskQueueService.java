package io.github.colinzhu.taskqueue;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.sqlclient.SqlConnection;

import java.time.Duration;

/**
 * <pre>
 * Task Queue Service to:
 * 1. enqueue - put a task into a queue
 * 2. finish - remove the task from the queue
 * 3. reenqueue - put the task back to the queue with a process delay time
 * </pre>
 */
public interface TaskQueueService {
    static TaskQueueService taskQueue() {
        return TaskQueueServiceDbImpl.getInstance();
    }
    static TaskQueueService taskQueue(Vertx vertx) {
        return TaskQueueServiceDbEventBusImpl.getInstance(vertx);
    }

    /**
     * To create a task into task queue
     *
     * @param sqlConnection DB transaction
     * @param queueName     queue name
     * @param refNumber     reference number of payload
     * @param payload       payload object which will be marshalled to json string
     * @param <T>           task payload type
     * @return future of a task id
     */
    <T> Future<Long> enqueue(SqlConnection sqlConnection, String queueName, String refNumber, T payload);

    /**
     * To create a task into task queue
     *
     * @param sqlConnection DB transaction
     * @param queueName     queue name
     * @param refNumber     reference number of payload
     * @param payload       payload object which will be marshalled to json string
     * @param delay         process delay time after putting into the queue
     * @param <T>           task payload type
     * @return future of a task which has been stored into task queue
     */
    <T> Future<Long> enqueue(SqlConnection sqlConnection, String queueName, String refNumber, T payload, Duration delay);

    /**
     * To finish the task (remove from task queue)
     * @param sqlConnection DB transaction
     * @param taskId the task ID
     * @return future of number of task updated / removed
     */
    Future<Integer> finish(SqlConnection sqlConnection, long taskId);

    /**
     * Update the task so that it can be processed again later
     * @param sqlConnection DB transaction
     * @param taskId the task ID
     * @param delay process delay time after putting into the queue
     * @return future of number of task updated
     */
    Future<Integer> reenqueue(SqlConnection sqlConnection, long taskId, Duration delay);
}
