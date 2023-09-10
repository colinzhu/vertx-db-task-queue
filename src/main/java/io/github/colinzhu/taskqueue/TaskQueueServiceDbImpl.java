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
import java.util.function.Function;

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
    public <T> Function<SqlConnection, Future<Task<T>>> enqueue(Function<SqlConnection, Future<T>> function, String queueName, Function<T, String> refExtractor) {
        return enqueue(function, queueName, refExtractor, Duration.ZERO);
    }

    @Override
    public <T> Function<SqlConnection, Future<Task<T>>> enqueue(Function<SqlConnection, Future<T>> function, String queueName, Function<T, String> refExtractor, Duration processDelay) {
        return sqlConnection -> function.apply(sqlConnection)
                .compose(result -> enqueue(sqlConnection, queueName, refExtractor.apply(result), result, processDelay));
    }
    @Override
    public <T> Function<SqlConnection, Future<T>> reenqueue(Function<SqlConnection, Future<T>> function, long taskId, Duration delay) {
        return sqlConnection -> {
            Future<T> f = function.apply(sqlConnection);
            return f.compose(result -> reenqueue(sqlConnection, taskId, delay)).map(result -> f.result());
        };
    }
    @Override
    public <T> Function<SqlConnection, Future<T>> finish(Function<SqlConnection, Future<T>> function, long taskId) {
        return sqlConnection -> {
            Future<T> f = function.apply(sqlConnection);
                    return f.compose(result -> finish(sqlConnection, taskId)).map(result -> f.result());
        };
    }
    <T> Future<Task<T>> enqueue(SqlConnection sqlConnection, String queueName, String refNumber, T payload, Duration processDelay) {
        String payloadStr;
        try {
            payloadStr = objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            return Future.failedFuture(e);
        }
        return taskEntityRepo.insert(sqlConnection, queueName, refNumber, payloadStr, processDelay)
                .map(taskEntity -> new Task<>(taskEntity.getId(), payload));
    }
    Future<Integer> fail(SqlConnection sqlConnection, long taskId) {
        return taskEntityRepo.updateStatusToError(sqlConnection, taskId);
    }
    private Future<Integer> finish(SqlConnection sqlConnection, long taskId) {
        return taskEntityRepo.delete(sqlConnection, taskId);
    }
    private Future<Integer> reenqueue(SqlConnection sqlConnection, long taskId, Duration delay) {
        return taskEntityRepo.reenqueue(sqlConnection, taskId, delay);
    }
}
