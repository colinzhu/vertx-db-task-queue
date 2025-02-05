package io.github.colinzhu.taskqueue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.github.colinzhu.taskqueue.internal.TaskRepo;
import io.github.colinzhu.taskqueue.polling.Task;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.sqlclient.SqlConnection;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;

import static io.github.colinzhu.taskqueue.internal.TaskStatus.COMPLETED;
import static io.github.colinzhu.taskqueue.internal.TaskStatus.PROCESSING;

@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
class TaskQueueServiceImpl implements TaskQueueService {
    private final Vertx vertx;
    private final TaskRepo taskRepo = new TaskRepo();

    /**
     * disable SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, so the date time format will be:
     * OffsetDateTime: "2023-09-15T20:09:06.972733991+08:00", instead: 1694779746.972733991
     * LocalDateTime: "2023-09-15T20:09:06.9727829", instead: [2023,9,15,20,9,6,972782900]
     */
    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule()).disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

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
        return taskRepo.insert(sqlConnection, queueName, refNumber, payloadStr, processDelay)
                .map(task -> {
                    if (Duration.ZERO.equals(processDelay)) {
                        vertx.eventBus().send("poller." + queueName, task.getId());
                        log.debug("New task notification sent to event bus, address={}, taskId={}", "poller." + queueName, task.getId());
                    }
                    return task.getId();
                });
    }

    @Override
    public <T> Future<Integer> complete(SqlConnection sqlConnection, Task<T> task) {
        return complete(sqlConnection, task, null);
    }

    @Override
    public <T> Future<Integer> complete(SqlConnection sqlConnection, Task<T> task, String processResult) {
        return taskRepo.updateStatusFromWithResult(sqlConnection, task.getId(), PROCESSING, COMPLETED, processResult);
    }

    @Override
    public <T> Future<Integer> completeDelete(SqlConnection sqlConnection, Task<T> task) {
        return taskRepo.completeDelete(sqlConnection, task.getId());
    }

    @Override
    public <T> Future<Integer> reenqueue(SqlConnection sqlConnection, Task<T> task, Duration delay) {
        return reenqueue(sqlConnection, task, delay, null);
    }

    @Override
    public <T> Future<Integer> reenqueue(SqlConnection sqlConnection, Task<T> task, Duration delay, String processResult) {
        return taskRepo.reenqueue(sqlConnection, task.getId(), delay, processResult);
        // not sending new task notification to poller, the task will wait for some time before being processed, max noTaskInterval
        // because:
        // 1. usually reenqueue should have a delay
        // 2. there is no direct queue name to send notification
    }
}
