package io.github.colinzhu.taskqueue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.vertx.core.Future;
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
class TaskQueueServiceDbImpl implements TaskQueueService {
    private static final TaskQueueService instance = new TaskQueueServiceDbImpl();
    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    public static TaskQueueService getInstance() {
        return instance;
    }

    private final TaskRepo taskRepo = TaskRepo.getInstance();
    private TaskQueueServiceDbImpl() {
    }

    public <T> Future<Task<T>> enqueue(SqlConnection sqlConnection, String queueName, String refNumber, T payload, Duration processDelay) {
        String payloadStr;
        try {
            payloadStr = objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            return Future.failedFuture(e);
        }
        return taskRepo.insert(sqlConnection, queueName, refNumber, payloadStr, processDelay)
                .map(taskEntity -> new Task<>(taskEntity.getId(), payload));
    }

    @Override
    public Future<Integer> finish(SqlConnection sqlConnection, long taskId) {
        return taskRepo.delete(sqlConnection, taskId);
    }

    @Override
    public Future<Integer> fail(SqlConnection sqlConnection, long taskId) {
        return taskRepo.updateStatusToError(sqlConnection, taskId);
    }

    @Override
    public Future<Integer> reenqueue(SqlConnection sqlConnection, long taskId, Duration delay) {
        return taskRepo.reenqueue(sqlConnection, taskId, delay);
    }
}
