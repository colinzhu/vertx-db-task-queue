package io.github.colinzhu.dbqueue.example;

import io.github.colinzhu.dbqueue.api.Task;
import io.github.colinzhu.dbqueue.api.taskqueue.TaskQueue;
import io.vertx.core.Future;
import io.vertx.jdbcclient.JDBCPool;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.function.Function;

@Slf4j
@RequiredArgsConstructor
public class ExampleTaskProcessor implements Function<Task, Future<?>> {
    private final JDBCPool pool;
    private TaskQueue taskQueue = TaskQueue.taskQueue();
    @Override
    public Future<?> apply(Task task) {
        return pool.withTransaction(sqlConnection -> {
            // do something with DB, e.g. update business entity table
            log.info("Processing task:{}", task);
            log.info("Process completed task:{}", task);
            // handle the task e.g. close the task
            return taskQueue.success(sqlConnection, task.getId());
        });
    }
}
