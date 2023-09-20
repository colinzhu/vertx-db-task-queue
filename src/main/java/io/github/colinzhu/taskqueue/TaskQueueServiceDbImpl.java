package io.github.colinzhu.taskqueue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
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
    protected final TaskEntityRepo taskEntityRepo;

    /**
     * disable SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, so the date time format will be:
     * OffsetDateTime: "2023-09-15T20:09:06.972733991+08:00", instead: 1694779746.972733991
     * LocalDateTime: "2023-09-15T20:09:06.9727829", instead: [2023,9,15,20,9,6,972782900]
     */
    protected final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule()).disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    private static final TaskQueueServiceDbImpl instance = new TaskQueueServiceDbImpl(TaskEntityRepo.getInstance());

    static TaskQueueServiceDbImpl getInstance() {
        return instance;
    }

    @Override
    public <T> Future<Long> enqueue(SqlConnection sqlConnection, String queueName, String refNumber, T payload) {
        return enqueue(sqlConnection, queueName, refNumber, payload, Duration.ZERO);
    }
    @Override
    public <T> Future<Long> enqueue(SqlConnection sqlConnection, String queueName, String refNumber, T payload, Duration processDelay) {
        String payloadStr;
        try {
            payloadStr = objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            return Future.failedFuture(e);
        }
        return taskEntityRepo.insert(sqlConnection, queueName, refNumber, payloadStr, processDelay)
                .map(TaskEntity::getId);
    }

    @Override
    public Future<Integer> finish(SqlConnection sqlConnection, long taskId) {
        return taskEntityRepo.finish(sqlConnection, taskId);
        //return taskEntityRepo.updateStatus(sqlConnection, taskId, "COMPLETED");
    }

    @Override
    public Future<Integer> reenqueue(SqlConnection sqlConnection, long taskId, Duration delay) {
        return taskEntityRepo.reenqueue(sqlConnection, taskId, delay);
    }

    public Future<Integer> fail(SqlConnection sqlConnection, long taskId) {
        return taskEntityRepo.updateStatusToError(sqlConnection, taskId);
    }
}
