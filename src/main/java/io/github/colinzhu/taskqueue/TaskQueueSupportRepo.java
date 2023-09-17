package io.github.colinzhu.taskqueue;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.SqlResult;
import io.vertx.sqlclient.templates.SqlTemplate;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
class TaskQueueSupportRepo {
    private static final TaskQueueSupportRepo instance = new TaskQueueSupportRepo();
    static TaskQueueSupportRepo getInstance() {
        return instance;
    }

    // below for support only
    private static final String SQL_RE_ENQUEUE_ERR_BATCH = "UPDATE TASKS SET STATUS = 'CREATED', NEXT_PROCESS_TIME = #{newNextProcessTime}, LAST_UPDATE_TIME = CURRENT_TIMESTAMP() WHERE ID IN ({idList}) AND STATUS = 'ERROR'";
    private static final String SQL_SEARCH_QNAME_STATUS = "SELECT * FROM TASKS WHERE QUEUE_NAME = #{queueName} AND STATUS = #{status} ORDER BY NEXT_PROCESS_TIME FETCH FIRST #{batchSize} ROWS ONLY";
    private static final String SQL_COUNT_QNAME_STATUS = "SELECT QUEUE_NAME, STATUS, COUNT(ID) ROWCOUNT FROM TASKS GROUP BY QUEUE_NAME, STATUS ORDER BY QUEUE_NAME, STATUS";
    private static final String SQL_UPDATE_STATUS_BATCH = "UPDATE TASKS SET STATUS = #{toStatus} WHERE ID IN ({idList}) AND STATUS = #{fromStatus}";

    // for support only
    Future<Integer> reenqueueErrorTasks(SqlConnection sqlConnection, Set<Long> taskIds) {
        long start = System.currentTimeMillis();
        OffsetDateTime newNextProcessTime = OffsetDateTime.now();
        String idValues = taskIds.stream().map(Object::toString).collect(Collectors.joining(","));
        String sql = SQL_RE_ENQUEUE_ERR_BATCH.replace("{idList}", idValues);
        return SqlTemplate.forUpdate(sqlConnection, sql)
                .execute(Map.of("newNextProcessTime", newNextProcessTime))
                .map(SqlResult::rowCount)
                .onSuccess(updateCount -> log.info("task(s) reenqueueErrorTasks: updateCount={}, taskIds={}, nextProcessTime={}, time={}ms", updateCount, taskIds, newNextProcessTime, System.currentTimeMillis() - start))
                .onFailure(err -> log.error("task(s) reenqueueErrorTasks failed: taskIds={}, time:{}ms", taskIds, System.currentTimeMillis() - start, err));
    }

    // only for support
    Future<Integer> updateStatusFromToBatch(SqlConnection sqlConnection, Set<Long> taskIds, String fromStatus, String toStatus) {
        long start = System.currentTimeMillis();
        String idValues = taskIds.stream().map(Object::toString).collect(Collectors.joining(","));
        String sql = SQL_UPDATE_STATUS_BATCH.replace("{idList}", idValues);
        return SqlTemplate.forUpdate(sqlConnection, sql)
                .execute(Map.of("fromStatus", fromStatus, "toStatus", toStatus))
                .map(SqlResult::rowCount)
                .onSuccess(sqlResult -> log.info("task updateStatusFromToBatch: taskId={}, fromStatus={}, toStatus={}, time={}ms", taskIds, fromStatus, toStatus, System.currentTimeMillis() - start))
                .onFailure(err -> log.error("task(s) updateStatusFromToBatch failed: taskIds={}, time:{}ms", taskIds, System.currentTimeMillis() - start, err));
    }

    // for support only
    Future<List<TaskEntity>> searchByQueueNameAndStatus(SqlConnection sqlConnection, String queueName, String status, int batchSize) {
        long start = System.currentTimeMillis();
        return SqlTemplate.forQuery(sqlConnection, SQL_SEARCH_QNAME_STATUS)
                .execute(Map.of("queueName", queueName, "status", status, "batchSize", batchSize))
                .map(rows -> {
                    List<TaskEntity> records = new ArrayList<>();
                    rows.forEach(row -> records.add(mapRowToTaskEntity(row)));
                    if (rows.size() > 0) {
                        log.info("task searchByQueueNameAndStatus: queue={}, status={}, count={}, time={}ms", queueName, status, records.size(), System.currentTimeMillis() - start);
                    }
                    return records;
                })
                .onFailure(err -> log.error("task searchByQueueNameAndStatus failed: queue={}, status={}, time={}ms", queueName, status, System.currentTimeMillis() - start, err));
    }

    // for support only
    Future<List<JsonObject>> countGroupByQueueNameAndStatus(SqlConnection sqlConnection) {
        long start = System.currentTimeMillis();
        return sqlConnection.query(SQL_COUNT_QNAME_STATUS)
                .execute()
                .map(rows -> {
                    List<JsonObject> records = new ArrayList<>();
                    rows.forEach(row -> records.add(mapRowToCountRecord(row)));
                    if (rows.size() > 0) {
                        log.info("task searchByQueueNameAndStatus: count={}, time={}ms", records.size(), System.currentTimeMillis() - start);
                    }
                    return records;
                })
                .onFailure(err -> log.error("task searchByQueueNameAndStatus failed: time={}ms", System.currentTimeMillis() - start, err));
    }

    private static JsonObject mapRowToCountRecord(Row row) {
        JsonObject json = new JsonObject();
        json.put("queueName", row.getString("QUEUE_NAME"));
        json.put("status", row.getString("STATUS"));
        json.put("count", row.getLong("ROWCOUNT"));
        return json;
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
