package io.github.colinzhu.taskqueue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.vertx.core.Future;
import io.vertx.sqlclient.SqlConnection;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.function.Function;

@Slf4j
class TaskQueueServiceDbImpl implements TaskQueueService {
    private static final TaskQueueService instance = new TaskQueueServiceDbImpl();
    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    public static TaskQueueService getInstance() {
        return instance;
    }

    private final TaskEntityRepo taskEntityRepo = TaskEntityRepo.getInstance();
    TaskQueueServiceDbImpl() {
    }

    @Override
    public <T> Function<SqlConnection, Future<Task<T>>> enqueue(Function<SqlConnection, Future<T>> function, String queueName, Function<T, String> refExtractor, Duration processDelay) {
        return sqlConnection -> function.apply(sqlConnection)
                .compose(result -> enqueue(sqlConnection, queueName, refExtractor.apply(result), result, processDelay));
    }
    @Override
    public Function<SqlConnection, Future<Integer>> reenqueue(Function<SqlConnection, Future<?>> function, long taskId, Duration delay) {
        return sqlConnection -> function.apply(sqlConnection)
                .compose(result -> reenqueue(sqlConnection, taskId, delay));
    }
    @Override
    public Function<SqlConnection, Future<Integer>> finish(Function<SqlConnection, Future<?>> function, long taskId) {
        return sqlConnection -> function.apply(sqlConnection)
                .compose(result -> finish(sqlConnection, taskId));
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

    
    private Future<Integer> finish(SqlConnection sqlConnection, long taskId) {
        return taskEntityRepo.delete(sqlConnection, taskId);
    }


    Future<Integer> fail(SqlConnection sqlConnection, long taskId) {
        return taskEntityRepo.updateStatusToError(sqlConnection, taskId);
    }


    private Future<Integer> reenqueue(SqlConnection sqlConnection, long taskId, Duration delay) {
        return taskEntityRepo.reenqueue(sqlConnection, taskId, delay);
    }
}
