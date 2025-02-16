package io.github.colinzhu.taskqueue.internal;

import io.vertx.core.Future;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.SqlResult;
import io.vertx.sqlclient.templates.SqlTemplate;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Map;

import static io.github.colinzhu.taskqueue.internal.TaskStatus.CREATED;

@Slf4j
public class TaskRepo {
    private static final String SQL_INSERT = "INSERT INTO TASKS (QUEUE_NAME, STATUS, ATTEMPT, PAYLOAD, REFERENCE_NUMBER, CREATE_TIME, NEXT_PROCESS_TIME, LAST_UPDATE_TIME) VALUES (#{queueName}, 'CREATED', 0, #{payload}, #{refNumber}, #{createTime}, #{nextProcessTime}, #{lastUpdateTime})";
    private static final String SQL_FINISH_DELETE = "DELETE TASKS WHERE ID = #{id} AND STATUS = 'PROCESSING'"; // only delete status in PROCESSING, in case updated by other already
    private static final String SQL_UPDATE_STATUS_FROM_WITH_RESULT_NULL = "UPDATE TASKS SET STATUS = #{newStatus}, LAST_UPDATE_TIME = #{now}, PROCESS_RESULT = NULL WHERE ID = #{id} and STATUS = #{oriStatus}";
    private static final String SQL_UPDATE_STATUS_FROM_WITH_RESULT = "UPDATE TASKS SET STATUS = #{newStatus}, LAST_UPDATE_TIME = #{now}, PROCESS_RESULT = #{processResult} WHERE ID = #{id} and STATUS = #{oriStatus}";
    private static final String SQL_RE_ENQUEUE_WITH_RESULT_NULL = "UPDATE TASKS SET STATUS = 'CREATED', POLLER_INSTANCE = NULL, NEXT_PROCESS_TIME = #{newNextProcessTime}, LAST_UPDATE_TIME = #{now}, PROCESS_RESULT = NULL WHERE ID = #{id} AND STATUS = 'PROCESSING'";
    private static final String SQL_RE_ENQUEUE_WITH_RESULT = "UPDATE TASKS SET STATUS = 'CREATED', POLLER_INSTANCE = NULL, NEXT_PROCESS_TIME = #{newNextProcessTime}, LAST_UPDATE_TIME = #{now}, PROCESS_RESULT = #{processResult} WHERE ID = #{id} AND STATUS = 'PROCESSING'";

    static String truncateToUtf8ByteLength(String s, int maxBytes) {
        if (s == null) {
            return null;
        }
        CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();
        byte[] sba = s.getBytes(StandardCharsets.UTF_8);
        if (sba.length <= maxBytes) {
            return s;
        }
        // Ensure truncation by having byte buffer = maxBytes
        ByteBuffer bb = ByteBuffer.wrap(sba, 0, maxBytes);
        CharBuffer cb = CharBuffer.allocate(maxBytes);
        // Ignore an incomplete character
        decoder.onMalformedInput(CodingErrorAction.IGNORE);
        decoder.decode(bb, cb, true);
        decoder.flush(cb);
        return new String(cb.array(), 0, cb.position());
    }

    public Future<TaskEntity> insert(SqlConnection sqlConnection, String queueName, String refNumber, String payload, Duration processDelay) {
        long start = System.currentTimeMillis();
        OffsetDateTime now = OffsetDateTime.now();
        OffsetDateTime nextProcessTime = now.plus(processDelay);
        return SqlTemplate.forUpdate(sqlConnection, SQL_INSERT)
                .execute(Map.of(
                        "queueName", queueName,
                        "payload", payload,
                        "refNumber", refNumber,
                        "createTime", now,
                        "nextProcessTime", nextProcessTime,
                        "lastUpdateTime", now))
                .map(sqlResult -> new TaskEntity(
                        sqlResult.property(JDBCPool.GENERATED_KEYS).getLong(0),
                        refNumber,
                        queueName,
                        CREATED,
                        0L,
                        null,
                        now,
                        nextProcessTime,
                        now,
                        payload,
                        null
                ))
                .onSuccess(task -> log.info("task inserted: {}, processDelay={}, time={}ms", task, processDelay, System.currentTimeMillis() - start))
                .onFailure(err -> log.info("task insert failed: queue={}, refNumber={}, time={}ms", queueName, refNumber, System.currentTimeMillis() - start, err));
    }

    public Future<Integer> completeDelete(SqlConnection sqlConnection, long taskId) {
        long start = System.currentTimeMillis();
        return SqlTemplate.forUpdate(sqlConnection, SQL_FINISH_DELETE)
                .execute(Map.of("id", taskId))
                .map(SqlResult::rowCount)
                .map(deleteCount -> {
                    if (0 == deleteCount) {
                        throw new IllegalStateException(String.format("task delete failed: taskId=%s, deleteCount=0, expected=1, maybe already updated/deleted by another poller.", taskId));
                    } else {
                        return deleteCount;
                    }
                })
                .onSuccess(sqlResult -> log.info("task deleted: taskId={}, time={}ms", taskId, System.currentTimeMillis() - start));
    }

    public Future<Integer> updateStatusFromWithResult(SqlConnection sqlConnection, long taskId, TaskStatus oriStatus, TaskStatus newStatus, String processResult) {
        long start = System.currentTimeMillis();
        String truncatedResult = truncateToUtf8ByteLength(processResult, 4000);
        String sql;
        Map<String, Object> parameters;
        if (null == truncatedResult) {
            sql = SQL_UPDATE_STATUS_FROM_WITH_RESULT_NULL;
            parameters = Map.of("id", taskId, "oriStatus", oriStatus.name(), "newStatus", newStatus.name(), "now", OffsetDateTime.now());
        } else {
            sql = SQL_UPDATE_STATUS_FROM_WITH_RESULT;
            parameters = Map.of("id", taskId, "oriStatus", oriStatus.name(), "newStatus", newStatus.name(), "now", OffsetDateTime.now(), "processResult", truncatedResult);
        }

        return executeStatusUpdateWithRetry(sqlConnection, sql, parameters, taskId, newStatus, start, 3)
                .onSuccess(sqlResult -> log.info("task status updated to '{}': taskId={}, time={}ms", newStatus, taskId, System.currentTimeMillis() - start));
    }

    private Future<Integer> executeStatusUpdateWithRetry(SqlConnection sqlConnection, String sql, Map<String, Object> parameters, long taskId, TaskStatus newStatus, long start, int retries) {
        return SqlTemplate.forUpdate(sqlConnection, sql)
                .execute(parameters)
                .map(SqlResult::rowCount)
                .map(updateCount -> {
                    if (0 == updateCount) {
                        throw new IllegalStateException(String.format("task update status to '%s' failed: taskId=%s, updateCount=0, expected=1, maybe already updated/deleted by another poller.", newStatus.name(), taskId));
                    } else {
                        return updateCount;
                    }
                })
                .recover(err -> {
                    if (err instanceof IllegalStateException || retries <= 0) {
                        return Future.failedFuture(err);
                    } else {
                        log.warn("task status update to '{}' failed with unexpected exception, will retry, taskId={}", newStatus.name(), taskId, err);
                        return executeStatusUpdateWithRetry(sqlConnection, sql, parameters, taskId, newStatus, start, retries - 1)
                                .onSuccess(sqlResult -> log.info("task status updated to '{}' successfully (after retried): taskId={}, time={}ms", newStatus.name(), taskId, System.currentTimeMillis() - start));
                    }
                });
    }

    public Future<Integer> reenqueue(SqlConnection sqlConnection, Long taskId, Duration nextProcessDelay, String processResult) {
        long start = System.currentTimeMillis();
        OffsetDateTime now = OffsetDateTime.now();
        OffsetDateTime newNextProcessTime = now.plusSeconds(nextProcessDelay.getSeconds());

        String truncatedResult = truncateToUtf8ByteLength(processResult, 4000);
        String sql;
        Map<String, Object> map;
        if (null == truncatedResult) {
            sql = SQL_RE_ENQUEUE_WITH_RESULT_NULL;
            map = Map.of("id", taskId, "now", now, "newNextProcessTime", newNextProcessTime);
        } else {
            sql = SQL_RE_ENQUEUE_WITH_RESULT;
            map = Map.of("id", taskId, "now", now, "newNextProcessTime", newNextProcessTime, "processResult", truncatedResult);
        }

        return SqlTemplate.forUpdate(sqlConnection, sql)
                .execute(map)
                .map(SqlResult::rowCount)
                .map(updateCount -> {
                    if (0 == updateCount) {
                        throw new IllegalStateException(String.format("task reenqueue failed: taskId=%s, nextProcessTime=%s, updateCount=0. expected=1, maybe already updated/deleted by another poller.", taskId, newNextProcessTime));
                    } else {
                        return updateCount;
                    }
                })
                .onSuccess(sqlResult -> log.info("task reenqueued: taskId={}, nextProcessTime={}, time={}ms", taskId, newNextProcessTime, System.currentTimeMillis() - start));
    }


}
