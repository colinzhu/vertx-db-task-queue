package io.github.colinzhu.taskqueue.example;

import io.github.colinzhu.taskqueue.Task;
import io.github.colinzhu.taskqueue.manager.TaskQueueManager;
import io.vertx.core.Future;
import io.vertx.jdbcclient.JDBCPool;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.function.Function;

@Slf4j
@RequiredArgsConstructor
public class PaymentCheckTaskProcessor implements Function<Task, Future<?>> {
    private final JDBCPool pool;
    private final TaskQueueManager taskQueueManager = TaskQueueManager.taskQueue();
    @Override
    public Future<?> apply(Task task) {
        return pool.withTransaction(sqlConnection -> {
            // do something with DB, e.g. update business entity table
            log.info("[taskId:{}] Process completed.", task.getId());
            // handle the task e.g. close the task
            return taskQueueManager.success(sqlConnection, task.getId());
        });
    }
}
