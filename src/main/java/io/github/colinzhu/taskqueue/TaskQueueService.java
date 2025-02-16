package io.github.colinzhu.taskqueue;

import io.github.colinzhu.taskqueue.polling.Task;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.sqlclient.SqlConnection;

import java.time.Duration;

/**
 * <pre>
 * Task Queue Service to:
 * 1. enqueue - put a task into a queue
 * 2. complete - complete a task from the queue
 * 3. reenqueue - put the task back to the queue with a process delay time
 * </pre>
 */
public interface TaskQueueService {
    static TaskQueueService taskQueue(Vertx vertx) {
        return new TaskQueueServiceImpl(vertx);
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

    /**
     * Completes a task without setting a process result.
     * @param sqlConnection DB transaction
     * @param task the task
     * @return future of number of task updated
     */
    <T> Future<Integer> complete(SqlConnection sqlConnection, Task<T> task);

    /**
     * Completes a task and sets a process result.
     * @param sqlConnection DB transaction
     * @param task the task
     * @param processResult the process result
     * @return future of number of task updated
     */
    <T> Future<Integer> complete(SqlConnection sqlConnection, Task<T> task, String processResult);

    /**
     * Completes a task by deleting it from the queue.
     * @param sqlConnection DB transaction
     * @param task the task
     * @return future of number of task deleted
     */
    <T> Future<Integer> completeDelete(SqlConnection sqlConnection, Task<T> task);

    /**
     * Reenqueues a task so that it can be processed again later.
     * @param sqlConnection DB transaction
     * @param task the task
     * @param delay process delay time after putting into the queue
     * @return future of number of task updated
     */
    <T> Future<Integer> reenqueue(SqlConnection sqlConnection, Task<T> task, Duration delay);

    /**
     * Reenqueues a task so that it can be processed again later and sets a process result.
     * @param sqlConnection DB transaction
     * @param task the task
     * @param delay process delay time after putting into the queue
     * @param processResult current process result before the next process
     * @return future of number of task updated
     */
    <T> Future<Integer> reenqueue(SqlConnection sqlConnection, Task<T> task, Duration delay, String processResult);
}
