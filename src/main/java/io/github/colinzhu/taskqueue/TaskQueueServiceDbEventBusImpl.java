package io.github.colinzhu.taskqueue;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.sqlclient.SqlConnection;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;

/**
 * QueueClient
 * task enqueue into queue
 * task dispatch to handler
 * task dequeue from queue (success, failure, reprocess)
 * <p>
 * Client put task into -> Message Broker dispatch -> Client dequeue task
 */
@Slf4j
class TaskQueueServiceDbEventBusImpl implements TaskQueueService {
    private final Vertx vertx;
    private final TaskQueueService dbImpl;
    private static TaskQueueService instance;

    public static TaskQueueService getInstance(Vertx vertx) {
        if (null == instance) {
            instance = new TaskQueueServiceDbEventBusImpl(vertx);
        }
        return instance;
    }

    private TaskQueueServiceDbEventBusImpl(Vertx vertx) {
        this.vertx = vertx;
        this.dbImpl = TaskQueueServiceDbImpl.getInstance();
    }

    public <T> Future<?> enqueue(SqlConnection sqlConnection, String queueName, String refNumber, T payload, Duration processDelay) {
        return dbImpl.enqueue(sqlConnection, queueName, refNumber, payload, processDelay)
                .map(task -> {
                    vertx.eventBus().send(queueName, task);
                    log.info("[{}]Task sent to event bus, refNumber:{}, taskId:{}, nextProcessDelay:{}",
                            queueName, refNumber, ((Task<?>)task).getId(), processDelay);
                    return task;
                });
    }

    @Override
    public Future<Integer> finish(SqlConnection sqlConnection, long taskId) {
        return dbImpl.finish(sqlConnection, taskId);
    }

    @Override
    public Future<Integer> fail(SqlConnection sqlConnection, long taskId) {
        return dbImpl.fail(sqlConnection, taskId);
    }

    @Override
    public Future<?> reenqueue(SqlConnection sqlConnection, long taskId, Duration delay) {
        return dbImpl.reenqueue(sqlConnection, taskId, delay);
    }
}
