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

    private final TaskEntityRepo taskEntityRepo = TaskEntityRepo.getInstance();
    private TaskQueueServiceDbImpl() {
    }

    public <T> Future<Task<T>> enqueue(SqlConnection sqlConnection, String queueName, String refNumber, T payload, Duration processDelay) {
        String payloadStr;
        try {
            payloadStr = objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            return Future.failedFuture(e);
        }
        return taskEntityRepo.insert(sqlConnection, queueName, refNumber, payloadStr, processDelay)
                .map(taskEntity -> new Task<>(taskEntity.getId(), payload));
    }

    @Override
    public Future<Integer> finish(SqlConnection sqlConnection, long taskId) {
        return taskEntityRepo.delete(sqlConnection, taskId);
    }

    @Override
    public Future<Integer> fail(SqlConnection sqlConnection, long taskId) {
        return taskEntityRepo.updateStatusToError(sqlConnection, taskId);
    }

    @Override
    public Future<Integer> reenqueue(SqlConnection sqlConnection, long taskId, Duration delay) {
        return taskEntityRepo.reenqueue(sqlConnection, taskId, delay);
    }
}
