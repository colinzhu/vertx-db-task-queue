package io.github.colinzhu.taskqueue;

import io.vertx.core.Future;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.SqlResult;
import io.vertx.sqlclient.templates.SqlTemplate;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
class TaskEntityRepo {
    private static final TaskEntityRepo instance = new TaskEntityRepo();

    static TaskEntityRepo getInstance() {
        return instance;
    }

    private static final String SQL_INSERT = "INSERT INTO TASKS (QUEUE_NAME, STATUS, ATTEMPT, PAYLOAD, REFERENCE_NUMBER, CREATE_TIME, NEXT_PROCESS_TIME, LAST_UPDATE_TIME) VALUES (#{queueName}, #{status}, #{attempt}, #{payload}, #{refNumber}, #{createTime}, #{nextProcessTime}, #{lastUpdateTime})";
    private static final String SQL_FINISH = "DELETE TASKS WHERE ID = #{id} AND STATUS = 'PROCESSING'"; // only delete status in PROCESSING, in case updated by other already
    private static final String SQL_UPDATE_STATUS = "UPDATE TASKS SET STATUS = #{newStatus}, LAST_UPDATE_TIME = #{now} WHERE ID = #{id}";
    private static final String SQL_SELECT_FOR_UPDATE = "SELECT * FROM TASKS WHERE ID IN (SELECT ID FROM TASKS WHERE STATUS IN ('CREATED','PROCESSING') AND QUEUE_NAME = #{queueName} AND NEXT_PROCESS_TIME <= #{now} ORDER BY NEXT_PROCESS_TIME FETCH FIRST #{batchSize} ROWS ONLY) AND STATUS IN ('CREATED','PROCESSING') AND NEXT_PROCESS_TIME <= #{now} FOR UPDATE SKIP LOCKED";
//    private static final String SQL_SELECT_FOR_UPDATE = "SELECT * FROM TASKS WHERE STATUS IN ('CREATED','PROCESSING') AND QUEUE_NAME = #{queueName} AND NEXT_PROCESS_TIME <= #{now} AND ROWNUM <= #{batchSize} FOR UPDATE SKIP LOCKED";
//    private static final String SQL_SELECT_FOR_UPDATE = "SELECT * FROM TASKS WHERE STATUS IN ('CREATED','PROCESSING') AND QUEUE_NAME = #{queueName} AND NEXT_PROCESS_TIME <= #{now} ORDER BY NEXT_PROCESS_TIME, ID FETCH FIRST #{batchSize} ROWS ONLY FOR UPDATE SKIP LOCKED";
    private static final String SQL_CHECK_OUT = "UPDATE TASKS SET ATTEMPT = ATTEMPT + 1, STATUS = 'PROCESSING', NEXT_PROCESS_TIME = #{newNextProcessTime}, LAST_UPDATE_TIME = #{now} WHERE ID IN ({idList})";
    private static final String SQL_RE_ENQUEUE = "UPDATE TASKS SET STATUS = 'CREATED', NEXT_PROCESS_TIME = #{newNextProcessTime}, LAST_UPDATE_TIME = #{now} WHERE ID = #{id} AND STATUS = 'PROCESSING'";

    Future<TaskEntity> insert(SqlConnection sqlConnection, String queueName, String refNumber, String payload, Duration processDelay) {
        return insert(sqlConnection, queueName, refNumber, payload, processDelay, "CREATED", 0L);
    }

    Future<TaskEntity> insert(SqlConnection sqlConnection, String queueName, String refNumber, String payload, Duration processDelay, String status, long attempt) {
        long start = System.currentTimeMillis();
        OffsetDateTime now = OffsetDateTime.now();
        OffsetDateTime nextProcessTime = now.plus(processDelay);
        return SqlTemplate.forUpdate(sqlConnection, SQL_INSERT)
                .execute(Map.of(
                        "queueName", queueName,
                        "status", status,
                        "attempt", attempt,
                        "payload", payload,
                        "refNumber", refNumber,
                        "createTime", now,
                        "nextProcessTime", nextProcessTime,
                        "lastUpdateTime", now))
                .map(sqlResult -> new TaskEntity(
                        sqlResult.property(JDBCPool.GENERATED_KEYS).getLong(0),
                        refNumber,
                        queueName,
                        status,
                        attempt,
                        now,
                        nextProcessTime,
                        now,
                        payload
                ))
                .onSuccess(task -> log.info("task inserted: {}, processDelay={}, time={}ms", task, processDelay, System.currentTimeMillis() - start))
                .onFailure(err -> log.info("task insert failed: queue={}, refNumber={}, time={}ms", queueName, refNumber, System.currentTimeMillis() - start, err));
    }

    Future<Integer> finish(SqlConnection sqlConnection, long taskId) {
        long start = System.currentTimeMillis();
        return SqlTemplate.forUpdate(sqlConnection, SQL_FINISH)
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

    Future<Integer> updateStatusToError(SqlConnection sqlConnection, long taskId) {
        return updateStatus(sqlConnection, taskId, "ERROR");
    }

    Future<Integer> updateStatus(SqlConnection sqlConnection, long taskId, String newStatus) {
        long start = System.currentTimeMillis();
        return SqlTemplate.forUpdate(sqlConnection, SQL_UPDATE_STATUS)
                .execute(Map.of("id", taskId, "newStatus", newStatus, "now", OffsetDateTime.now()))
                .map(SqlResult::rowCount)
                .map(updateCount -> {
                    if (0 == updateCount) {
                        throw new IllegalStateException(String.format("task update status to '%s' failed: taskId=%s, toStatus=%s, updateCount=0, expected=1, maybe already deleted by another poller.", newStatus, taskId, newStatus));
                    } else {
                        return updateCount;
                    }
                })
                .onSuccess(sqlResult -> log.info("task status updated to '{}': taskId={}, time={}ms", newStatus, taskId, System.currentTimeMillis() - start));
    }

    Future<List<TaskEntity>> checkout(SqlConnection sqlConnection, String queueName, int batchSize, Duration nextProcessDelay) {
        var taskList = checkoutSelect(sqlConnection, queueName, batchSize);
        return taskList
                .compose(records -> checkoutUpdate(sqlConnection, records.stream().map(TaskEntity::getId).collect(Collectors.toList()), nextProcessDelay))
                .map(records -> taskList.result())
                .onFailure(err -> log.error("task checkout failed: queue={}", queueName, err));
    }

    private Future<List<TaskEntity>> checkoutSelect(SqlConnection sqlConnection, String queueName, int batchSize) {
        long start = System.currentTimeMillis();
        return SqlTemplate.forQuery(sqlConnection, SQL_SELECT_FOR_UPDATE)
                .execute(Map.of("now", OffsetDateTime.now(), "queueName", queueName, "batchSize", batchSize))
                .onFailure(err -> log.error("task checkoutSelect failed: queue={}, time={}ms", queueName, System.currentTimeMillis() - start, err))
                .map(rows -> {
                    List<TaskEntity> records = new ArrayList<>();
                    rows.forEach(row -> records.add(mapRowToTaskEntity(row)));
                    if (rows.size() > 0) {
                        log.debug("task checkoutSelect: queue={}, count={}, taskList={}, time={}ms", queueName, records.size(), records, System.currentTimeMillis() - start);
                    }
                    return records;
                });
    }

    private Future<Integer> checkoutUpdate(SqlConnection sqlConnection, List<Long> taskIdList, Duration nextProcessDelay) {
        long start = System.currentTimeMillis();
        Future<Integer> future;
        if (taskIdList.isEmpty()) {
            future = Future.succeededFuture(0);
        } else {
            String idValues = taskIdList.stream().map(String::valueOf).collect(Collectors.joining(","));
            String sql = SQL_CHECK_OUT.replace("{idList}", idValues);
            OffsetDateTime newNextProcessTime = OffsetDateTime.now().plusSeconds(nextProcessDelay.getSeconds());
            future = SqlTemplate.forUpdate(sqlConnection, sql)
                    .execute(Map.of("now", OffsetDateTime.now(), "newNextProcessTime", newNextProcessTime))
                    .map(SqlResult::rowCount)
                    .map(updateCount -> {
                        if (0 == updateCount) {
                            throw new IllegalStateException(String.format("task checkoutUpdate failed: updateCount=0, expected=%d, taskIdList=%s ", taskIdList.size(), idValues));
                        } else {
                            return updateCount;
                        }
                    });
        }
        return future
                .onSuccess(updateCount -> {
                    if (updateCount > 0) {
                        log.debug("task checkoutUpdate updated: count={}, taskIdList={}, time={}ms", updateCount, taskIdList, System.currentTimeMillis() - start);
                    }
                });
    }


    Future<Integer> reenqueue(SqlConnection sqlConnection, Long taskId, Duration nextProcessDelay) {
        long start = System.currentTimeMillis();
        OffsetDateTime now = OffsetDateTime.now();
        OffsetDateTime newNextProcessTime = now.plusSeconds(nextProcessDelay.getSeconds());
        return SqlTemplate.forUpdate(sqlConnection, SQL_RE_ENQUEUE)
                .execute(Map.of("id", taskId, "now", now, "newNextProcessTime", newNextProcessTime))
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

    private static TaskEntity mapRowToTaskEntity(Row row) {
        return TaskEntity.builder()
                .id(row.getLong("ID"))
                .referenceNumber(row.getString("REFERENCE_NUMBER"))
                .queueName(row.getString("QUEUE_NAME"))
                .status(row.getString("STATUS"))
                .attempt(row.getLong("ATTEMPT"))
                .createTime(row.getOffsetDateTime("CREATE_TIME"))
                .nextProcessTime(row.getOffsetDateTime("NEXT_PROCESS_TIME"))
                .lastUpdateTime(row.getOffsetDateTime("LAST_UPDATE_TIME"))
                .payload(row.getString("PAYLOAD"))
                .build();
    }
}
