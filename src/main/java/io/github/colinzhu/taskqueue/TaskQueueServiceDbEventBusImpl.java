package io.github.colinzhu.taskqueue;

import com.fasterxml.jackson.core.JsonProcessingException;
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
 *
 * Pure DB TaskQueueService:
 * 1. insert task to db
 * 2. checkout - poller:
 *    2.1 select task from db (batch)
 *    2.2 update task in db (batch)
 * 3. finish task (delete / update to error)
 *
 * With event bus, step 2 can be reduced, and no poller waiting time.
 *
 */
@Slf4j
class TaskQueueServiceDbEventBusImpl extends TaskQueueServiceDbImpl {
    private final Vertx vertx;
    private static TaskQueueServiceDbEventBusImpl instance;

    static TaskQueueServiceDbEventBusImpl getInstance(Vertx vertx) {
        if (null == instance) {
            instance = new TaskQueueServiceDbEventBusImpl(TaskEntityRepo.getInstance(), vertx);
        }
        return instance;
    }

    private TaskQueueServiceDbEventBusImpl(TaskEntityRepo taskEntityRepo, Vertx vertx) {
        super(taskEntityRepo);
        this.vertx = vertx;
    }

    @Override
    public <T> Future<Long> enqueue(SqlConnection sqlConnection, String queueName, String refNumber, T payload) {
        throw new UnsupportedOperationException("For using event bus, please use method with `processDelay` parameter.");
    }

    public <T> Future<Long> enqueue(SqlConnection sqlConnection, String queueName, String refNumber, T payload, Duration processDelay) {
        if (processDelay.isZero()) {
            throw new IllegalArgumentException("For using event bus, processDelay cannot be zero");
        }
        String payloadStr;
        try {
            payloadStr = objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            return Future.failedFuture(e);
        }
        return taskEntityRepo.insert(sqlConnection, queueName, refNumber, payloadStr, processDelay, "PROCESSING", 1L)
                .map(task -> {
                    vertx.eventBus().send(queueName, new Task<>(task.getId(), task.getAttempt(), payload)); // for event bus as it doesn't have "DB checkout" so need to add 1 for attempt
                    log.info("Task sent to event bus, queueName={}, refNumber={}, taskId={}, nextProcessDelay={}",
                            queueName, refNumber, task.getId(), processDelay);
                    return task.getId();
                });
    }
}
