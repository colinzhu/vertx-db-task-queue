package io.github.colinzhu.taskqueue;

import io.vertx.core.Future;
import io.vertx.jdbcclient.JDBCPool;
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

    private static final String SQL_INSERT = "INSERT INTO TASKS (QUEUE_NAME, STATUS, PAYLOAD, REFERENCE_NUMBER, NEXT_PROCESS_TIME) VALUES (#{queueName}, 'CREATED', #{payload}, #{refNumber}, #{nextProcessTime})";
    private static final String SQL_DELETE = "DELETE TASKS WHERE ID = #{id}";
    private static final String SQL_UPDATE_STATUS = "UPDATE TASKS SET STATUS = #{status} WHERE ID = #{id}";
    private static final String SQL_SELECT_FOR_UPDATE = "SELECT * FROM TASKS WHERE ID IN (SELECT ID FROM TASKS WHERE STATUS IN ('CREATED','PROCESSING') AND QUEUE_NAME = #{queueName} AND NEXT_PROCESS_TIME <= CURRENT_TIMESTAMP() ORDER BY NEXT_PROCESS_TIME FETCH FIRST #{batchSize} ROWS ONLY) FOR UPDATE SKIP LOCKED";
//    private static final String SQL_SELECT_FOR_UPDATE = "SELECT * FROM TASKS WHERE STATUS IN ('CREATED','PROCESSING') AND QUEUE_NAME = #{queueName} AND NEXT_PROCESS_TIME <= CURRENT_TIMESTAMP() AND ROWNUM <= #{batchSize} FOR UPDATE SKIP LOCKED";
//    private static final String SQL_SELECT_FOR_UPDATE = "SELECT * FROM TASKS WHERE STATUS IN ('CREATED','PROCESSING') AND QUEUE_NAME = #{queueName} AND NEXT_PROCESS_TIME <= CURRENT_TIMESTAMP() ORDER BY NEXT_PROCESS_TIME, ID FETCH FIRST #{batchSize} ROWS ONLY FOR UPDATE SKIP LOCKED";
    private static final String SQL_CHECK_OUT = "UPDATE TASKS SET ATTEMPT = ATTEMPT + 1, STATUS = 'PROCESSING', NEXT_PROCESS_TIME = #{newNextProcessTime}, LAST_UPDATE_TIME = CURRENT_TIMESTAMP() WHERE ID IN ({idList})";
    private static final String SQL_RE_ENQUEUE = "UPDATE TASKS SET STATUS = 'CREATED', NEXT_PROCESS_TIME = #{newNextProcessTime}, LAST_UPDATE_TIME = CURRENT_TIMESTAMP() WHERE ID = #{id}";

    Future<TaskEntity> insert(SqlConnection sqlConnection, String queueName, String refNumber, String payload, Duration processDelay) {
        OffsetDateTime now = OffsetDateTime.now();
        OffsetDateTime nextProcessTime = now.plus(processDelay);
        return SqlTemplate.forUpdate(sqlConnection, SQL_INSERT)
                .execute(Map.of(
                        "queueName", queueName,
                        "payload", payload,
                        "refNumber", refNumber,
                        "nextProcessTime", nextProcessTime))
                .map(sqlResult -> new TaskEntity(
                        sqlResult.property(JDBCPool.GENERATED_KEYS).getLong(0),
                        refNumber,
                        queueName,
                        "CREATED",
                        0L,
                        now,
                        nextProcessTime,
                        now,
                        payload
                ))
                .onSuccess(task -> log.info("task inserted: {}, processDelay={}", task, processDelay))
                .onFailure(err -> log.info("task insert failed: queue={}, refNumber={}", queueName, refNumber, err));
    }

    Future<Integer> delete(SqlConnection sqlConnection, long taskId) {
        long start = System.currentTimeMillis();
        return SqlTemplate.forUpdate(sqlConnection, SQL_DELETE)
                .execute(Map.of("id", taskId))
                .map(SqlResult::rowCount)
                .map(deleteCount -> {
                    if (0 == deleteCount) {
                        throw new IllegalStateException(String.format("task delete failed: taskId=%s, deleteCount=0, expected=1, maybe already deleted by another poller.", taskId));
                    } else {
                        return deleteCount;
                    }
                })
                .onSuccess(sqlResult -> log.info("task deleted: taskId={}, time={}ms", taskId, System.currentTimeMillis() - start));
                //.onFailure(err -> log.error("[taskId:{}] fail to delete task. Time:{}ms", taskId, System.currentTimeMillis() - start, err));
    }

    Future<Integer> updateStatusToError(SqlConnection sqlConnection, long taskId) {
        return updateStatus(sqlConnection, taskId, "ERROR");
    }

    private Future<Integer> updateStatus(SqlConnection sqlConnection, long taskId, String status) {
        long start = System.currentTimeMillis();
        return SqlTemplate.forUpdate(sqlConnection, SQL_UPDATE_STATUS)
                .execute(Map.of("id", taskId, "status", status))
                .map(SqlResult::rowCount)
                .map(updateCount -> {
                    if (0 == updateCount) {
                        throw new IllegalStateException(String.format("task update failed: taskId=%s, toStatus=%s, updateCount=0, expected=1, maybe already deleted by another poller.", taskId, status));
                    } else {
                        return updateCount;
                    }
                })
                .onSuccess(sqlResult -> log.info("task updated: taskId={}, newStatus={}, time={}ms", taskId, status, System.currentTimeMillis() - start));
                //.onFailure(err -> log.error("[taskId:{}] fail to update status to [{}]. Time:{}ms", taskId, status, System.currentTimeMillis() - start, err));
    }

    Future<List<TaskEntity>> checkout(SqlConnection sqlConnection, String queueName, int batchSize, Duration nextProcessDelay) {
        var taskList = selectTasks(sqlConnection, queueName, batchSize);
        return taskList
                .compose(records -> checkout(sqlConnection, records.stream().map(TaskEntity::getId).collect(Collectors.toList()), nextProcessDelay))
                .map(records -> taskList.result())
                .onFailure(err -> log.error("task checkout failed: queue={}", queueName, err));
    }


    private Future<List<TaskEntity>> selectTasks(SqlConnection sqlConnection, String queueName, int batchSize) {
        long start = System.currentTimeMillis();
        return SqlTemplate.forQuery(sqlConnection, SQL_SELECT_FOR_UPDATE)
                .execute(Map.of("queueName", queueName, "batchSize", batchSize))
                .onFailure(err -> log.error("task select failed: queue={}, time={}ms", queueName, System.currentTimeMillis() - start, err))
                .map(rows -> {
                    List<TaskEntity> records = new ArrayList<>();
                    rows.forEach(row -> records.add(TaskEntity.builder()
                            .id(row.getLong("ID"))
                            .referenceNumber(row.getString("REFERENCE_NUMBER"))
                            .queueName(row.getString("REFERENCE_NUMBER"))
                            .status(row.getString("STATUS"))
                            .attempt(row.getLong("ATTEMPT"))
                            .createTime(row.getOffsetDateTime("CREATE_TIME"))
                            .nextProcessTime(row.getOffsetDateTime("NEXT_PROCESS_TIME"))
                            .lastUpdateTime(row.getOffsetDateTime("LAST_UPDATE_TIME"))
                            .payload(row.getString("PAYLOAD"))
                            .build()
                    ));
                    if (rows.size() > 0) {
                        log.debug("task selected: queue={}, count={}, taskIdList={}, time={}ms", queueName, records.size(), records.stream().map(TaskEntity::getId).collect(Collectors.toList()), System.currentTimeMillis() - start);
                    }
                    return records;
                });
    }

    private Future<Integer> checkout(SqlConnection sqlConnection, List<Long> taskIdList, Duration nextProcessDelay) {
        long start = System.currentTimeMillis();
        Future<Integer> future;
        if (taskIdList.isEmpty()) {
            future = Future.succeededFuture(0);
        } else {
            String idValues = taskIdList.stream().map(String::valueOf).collect(Collectors.joining(","));
            String sql = SQL_CHECK_OUT.replace("{idList}", idValues);
            OffsetDateTime newNextProcessTime = OffsetDateTime.now().plusSeconds(nextProcessDelay.getSeconds());
            future = SqlTemplate.forUpdate(sqlConnection, sql)
                    .execute(Map.of("newNextProcessTime", newNextProcessTime))
                    .map(SqlResult::rowCount)
                    .map(updateCount -> {
                        if (0 == updateCount) {
                            throw new IllegalStateException(String.format("task checkout failed: updateCount=0, expected=%d, taskIdList=%s ", taskIdList.size(), idValues));
                        } else {
                            return updateCount;
                        }
                    });
        }
        return future
                .onSuccess(updateCount -> log.debug("task checkout updated: count={}, taskIdList={}, time={}ms", updateCount, taskIdList, System.currentTimeMillis() - start));
                //.onFailure(err -> log.error("taskIdList:{} checkout - failed, time:{}ms", taskIdList, System.currentTimeMillis() - start, err));
    }


    Future<Integer> reenqueue(SqlConnection sqlConnection, Long taskId, Duration nextProcessDelay) {
        long start = System.currentTimeMillis();
        OffsetDateTime newNextProcessTime = OffsetDateTime.now().plusSeconds(nextProcessDelay.getSeconds());
        return SqlTemplate.forUpdate(sqlConnection, SQL_RE_ENQUEUE)
                .execute(Map.of("id", taskId, "newNextProcessTime", newNextProcessTime))
                .map(SqlResult::rowCount)
                .map(updateCount -> {
                    if (0 == updateCount) {
                        throw new IllegalStateException(String.format("task reenqueue failed: taskId=%s, nextProcessTime=%s, updateCount=0. expected=1, maybe already deleted by another poller.", taskId, newNextProcessTime));
                    } else {
                        return updateCount;
                    }
                })
                .onSuccess(sqlResult -> log.info("task reenqueued: taskId={}, nextProcessTime={}, time={}ms", taskId, newNextProcessTime, System.currentTimeMillis() - start));
                //.onFailure(err -> log.error("[taskId:{}] fail to reenqueue to nextProcessTime:[{}]. Time:{}ms", taskId, newNextProcessTime, System.currentTimeMillis() - start, err));
    }
}
