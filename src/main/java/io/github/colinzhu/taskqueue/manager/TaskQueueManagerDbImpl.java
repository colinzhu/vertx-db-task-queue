package io.github.colinzhu.taskqueue.manager;

import io.vertx.core.Future;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.templates.SqlTemplate;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.Map;

/**
 * QueueClient
 * task enqueue into queue
 * task dispatch to handler
 * task dequeue from queue (success, failure, reprocess)
 * <p>
 * Client put task into -> Message Broker dispatch -> Client dequeue task
 */
@Slf4j
class TaskQueueManagerDbImpl implements TaskQueueManager {
    private static final TaskQueueManager instance = new TaskQueueManagerDbImpl();

    public static TaskQueueManager getInstance() {
        return instance;
    }

    private TaskQueueManagerDbImpl() {
    }

    private static final String SQL_ENQUEUE = "INSERT INTO TASKS (QUEUE_NAME, STATUS, PAYLOAD, REFERENCE_NUMBER) VALUES (#{queueName}, 'NEW', #{payload}, #{refNumber})";
    private static final String SQL_SUCCESS = "DELETE TASKS WHERE ID = #{id}";

    public Future<?> enqueue(SqlConnection sqlConnection, String queueName, String refNumber, String payload, Duration processDelay) {
        return SqlTemplate.forUpdate(sqlConnection, SQL_ENQUEUE)
                .execute(Map.of("queueName", queueName, "payload", payload, "refNumber", refNumber))
                .onSuccess(sqlResult -> log.info("[{}]Task inserted, refNumber:{}, taskId:{}, nextProcessDelay:{}",
                        queueName, refNumber, sqlResult.property(JDBCPool.GENERATED_KEYS).getLong(0), processDelay))
                .onFailure(err -> log.info("[{}]Fail to insert task, refNumber:{}", queueName, refNumber, err));
    }

    @Override
    public Future<?> success(SqlConnection sqlConnection, long taskId) {
        long start = System.currentTimeMillis();
        return SqlTemplate.forUpdate(sqlConnection, SQL_SUCCESS)
                .execute(Map.of("id", taskId))
                .onSuccess(sqlResult -> log.info("[taskId:{}] task deleted. Time:{}ms", taskId, System.currentTimeMillis() - start))
                .onFailure(err -> log.error("[taskId:{}] fail to delete task. Time:{}ms", taskId, System.currentTimeMillis() - start, err));
    }

    @Override
    public Future<?> failure(SqlConnection sqlConnection, long taskId) {
        return Future.succeededFuture();
    }

    @Override
    public Future<?> reenqueue(SqlConnection sqlConnection, long taskId, Duration delay) {
        return Future.succeededFuture();
    }
}
