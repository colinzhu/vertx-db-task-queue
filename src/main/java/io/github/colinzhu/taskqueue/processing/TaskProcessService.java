package io.github.colinzhu.taskqueue.processing;

import io.vertx.core.Future;
import io.vertx.sqlclient.SqlConnection;

import java.time.Duration;

/**
 * <pre>
 * Task Process Service to:
 * 1. complete - complete a task from the queue
 * 2. reenqueue - put the task back to the queue with a process delay time
 * </pre>
 */
public interface TaskProcessService {
    static TaskProcessService getInstance() {
        return new TaskProcessServiceImpl();
    }

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
