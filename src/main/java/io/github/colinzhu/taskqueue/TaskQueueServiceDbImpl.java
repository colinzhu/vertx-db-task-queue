package io.github.colinzhu.taskqueue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.vertx.core.Future;
import io.vertx.sqlclient.SqlConnection;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;

@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
class TaskQueueServiceDbImpl implements TaskQueueService {
    private final TaskEntityRepo taskEntityRepo;
    private final ObjectMapper objectMapper;
    private static final TaskQueueServiceDbImpl instance = new TaskQueueServiceDbImpl(TaskEntityRepo.getInstance(),
            new ObjectMapper().registerModule(new JavaTimeModule()));

    static TaskQueueServiceDbImpl getInstance() {
        return instance;
    }

    @Override
    public <T> Future<Task<T>> enqueue(SqlConnection sqlConnection, String queueName, String refNumber, T payload) {
        return enqueue(sqlConnection, queueName, refNumber, payload, Duration.ZERO);
    }
    @Override
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
    public Future<Integer> reenqueue(SqlConnection sqlConnection, long taskId, Duration delay) {
        return taskEntityRepo.reenqueue(sqlConnection, taskId, delay);
    }

    Future<Integer> fail(SqlConnection sqlConnection, long taskId) {
        return taskEntityRepo.updateStatusToError(sqlConnection, taskId);
    }
}
