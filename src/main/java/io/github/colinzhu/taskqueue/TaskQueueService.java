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
    default  <T> Function<SqlConnection, Future<Task<T>>> enqueue(Function<SqlConnection, Future<T>> function, String queueName, Function<T, String> refExtractor) {
        return enqueue(function, queueName, refExtractor, Duration.ZERO);
    }

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
    Function<SqlConnection, Future<Integer>> finish(Function<SqlConnection, Future<?>> function, long taskId);

    /**
     * Accept a main function, append the logic to re-put the task into the queue (update task process delay time). The main function and task handling logic will be in the same transaction.
     * @param function the main function
     * @param taskId the task ID
     * @param delay process delay time
     * @return function which invokes the main function, and then create re-put the task into task queue (update task process delay time)
     */
    Function<SqlConnection, Future<Integer>> reenqueue(Function<SqlConnection, Future<?>> function, long taskId, Duration delay);
}
