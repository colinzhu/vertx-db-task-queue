package io.github.colinzhu.taskqueue.example;

import io.github.colinzhu.taskqueue.Task;
import io.github.colinzhu.taskqueue.TaskQueueManager;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.jdbcclient.JDBCPool;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.function.Function;
import java.util.random.RandomGenerator;

@Slf4j
@RequiredArgsConstructor
public class PaymentCheckTaskProcessor implements Function<Task, Future<?>>, Handler<Message<Task>> {
    private final Vertx vertx;
    private final JDBCPool pool;
    private final TaskQueueManager taskQueueManager = TaskQueueManager.taskQueue();
    @Override
    public Future<?> apply(Task task) {
        return pool.withTransaction(sqlConnection -> {
            // do something with DB, e.g. update business entity table
            Promise<Object> promise = Promise.promise();
            vertx.setTimer(RandomGenerator.getDefault().nextInt(1,1000), id -> {
                log.info("[taskId:{}] Process completed. Payload:{}", task.getId(), task.getPayload());
                promise.complete();
            });
            return promise.future().compose(res -> taskQueueManager.success(sqlConnection, task.getId()));
        });
    }

    @Override
    public void handle(Message<Task> message) {
        apply(message.body());
    }
}
