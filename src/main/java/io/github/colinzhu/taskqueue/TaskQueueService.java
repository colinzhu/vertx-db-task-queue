package io.github.colinzhu.taskqueue;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.sqlclient.SqlConnection;

import java.time.Duration;
import java.util.function.Function;

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
     * Accept a main function, append the logic to create a task into task queue. The main function and task handling logic will be in the same transaction.
     * @param function the main function to return Future of task payload
     * @param queueName queue name
     * @param refExtractor function to extract reference number from task payload
     * @return function which invokes the main function, and then create a task into task queue
     * @param <T> task payload type
     */
     <T> Function<SqlConnection, Future<Task<T>>> enqueue(Function<SqlConnection, Future<T>> function, String queueName, Function<T, String> refExtractor);

    /**
     * Accept a main function, append the logic to create a task into task queue. The main function and task handling logic will be in the same transaction.
     * @param function the main function to return Future of task payload
     * @param queueName queue name
     * @param refExtractor function to extract reference number from task payload
     * @param processDelay process delay time
     * @return function which invokes the main function, and then create a task into task queue
     * @param <T> task payload type
     */
    <T> Function<SqlConnection, Future<Task<T>>> enqueue(Function<SqlConnection, Future<T>> function, String queueName, Function<T, String> refExtractor, Duration processDelay);

    /**
     * Accept a main function, append the logic to finish the task (remove from task queue). The main function and task handling logic will be in the same transaction.
     * @param function the main function
     * @param taskId the task ID
     * @return function which invokes the main function, and then finish the task (remove from task queue)
     */
    <T> Function<SqlConnection, Future<T>> finish(Function<SqlConnection, Future<T>> function, long taskId);

    /**
     * Accept a main function, append the logic to re-put the task into the queue (update task process delay time). The main function and task handling logic will be in the same transaction.
     * @param function the main function
     * @param taskId the task ID
     * @param delay process delay time
     * @return function which invokes the main function, and then create re-put the task into task queue (update task process delay time)
     */
    <T> Function<SqlConnection, Future<T>> reenqueue(Function<SqlConnection, Future<T>> function, long taskId, Duration delay);
}
