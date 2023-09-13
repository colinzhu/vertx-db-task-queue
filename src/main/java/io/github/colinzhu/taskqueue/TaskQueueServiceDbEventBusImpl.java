package io.github.colinzhu.taskqueue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
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
class TaskQueueServiceDbEventBusImpl extends TaskQueueServiceDbImpl {
    private final Vertx vertx;
    private static TaskQueueServiceDbEventBusImpl instance;

    static TaskQueueServiceDbEventBusImpl getInstance(Vertx vertx) {
        if (null == instance) {
            instance = new TaskQueueServiceDbEventBusImpl(TaskEntityRepo.getInstance(),
                    new ObjectMapper().registerModule(new JavaTimeModule()), vertx);
        }
        return instance;
    }

    private TaskQueueServiceDbEventBusImpl(TaskEntityRepo taskEntityRepo, ObjectMapper objectMapper, Vertx vertx) {
        super(taskEntityRepo, objectMapper);
        this.vertx = vertx;
    }

    @Override
    public <T> Future<Task<T>> enqueue(SqlConnection sqlConnection, String queueName, String refNumber, T payload) {
        throw new UnsupportedOperationException("For using event bus, please use method with `processDelay` parameter.");
    }

    public <T> Future<Task<T>> enqueue(SqlConnection sqlConnection, String queueName, String refNumber, T payload, Duration processDelay) {
        if (processDelay.isZero()) {
            throw new IllegalArgumentException("For using event bus, processDelay cannot be zero");
        }
        return super.enqueue(sqlConnection, queueName, refNumber, payload, processDelay)
                .map(task -> {
                    vertx.eventBus().send(queueName, task);
                    log.info("[{}]Task sent to event bus, refNumber:{}, taskId:{}, nextProcessDelay:{}",
                            queueName, refNumber, task.getId(), processDelay);
                    return task;
                });
    }
}
