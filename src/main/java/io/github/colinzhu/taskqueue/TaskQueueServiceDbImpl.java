package io.github.colinzhu.taskqueue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.vertx.core.Future;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.SqlConnection;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.List;
import java.util.function.Function;

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
    private static TaskQueueServiceDbImpl instance;
    private final JDBCPool pool;
    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    private final TaskRepo taskRepo = TaskRepo.getInstance();

    public static TaskQueueService getInstance(JDBCPool pool) {
        if (null == instance) {
            instance = new TaskQueueServiceDbImpl(pool);
        }
        return instance;
    }
    private TaskQueueServiceDbImpl(JDBCPool pool) {
        this.pool = pool;
    }

    public <T> Future<T> withTaskQueueTxn(Function<SqlConnection, Future<T>> function, Function<SqlConnection, Future<Integer>> taskFunction) {
        return pool.withTransaction(sqlConnection -> function.apply(sqlConnection)
                .onSuccess(res -> taskFunction.apply(sqlConnection)));
    }

    public <T> Future<Task<String>> enqueue(SqlConnection sqlConnection, String queueName, String refNumber, T payload, Duration processDelay) {
        String payloadStr;
        try {
            payloadStr = objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            return Future.failedFuture(e);
        }
        return taskRepo.insert(sqlConnection, queueName, refNumber, payloadStr, processDelay);
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
        return taskRepo.updateNextProcessTime(sqlConnection, List.of(taskId), delay);
    }
}
