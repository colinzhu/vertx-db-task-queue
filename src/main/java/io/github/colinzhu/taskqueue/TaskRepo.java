package io.github.colinzhu.taskqueue;

import io.vertx.core.Future;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.SqlResult;
import io.vertx.sqlclient.templates.SqlTemplate;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
class TaskRepo {
    private static final TaskRepo instance = new TaskRepo();

    public static TaskRepo getInstance() {
        return instance;
    }

    private TaskRepo() {
    }
    private static final String SQL_INSERT = "INSERT INTO TASKS (QUEUE_NAME, STATUS, PAYLOAD, REFERENCE_NUMBER, NEXT_PROCESS_TIME) VALUES (#{queueName}, 'NEW', #{payload}, #{refNumber}, #{nextProcessTime})";
    private static final String SQL_DELETE = "DELETE TASKS WHERE ID = #{id}";
    private static final String SQL_UPDATE_STATUS = "UPDATE TASKS SET STATUS = #{status} WHERE ID = #{id}";

    private static final String SQL_SELECT_FOR_UPDATE = "SELECT * FROM TASKS WHERE STATUS IN ('NEW','PROCESSING') AND QUEUE_NAME = #{queueName} AND NEXT_PROCESS_TIME <= CURRENT_TIMESTAMP() ORDER BY NEXT_PROCESS_TIME, ID FETCH FIRST #{batchSize} ROWS ONLY FOR UPDATE SKIP LOCKED";
    private static final String SQL_UPDATE_NEXT_PROCESS = "UPDATE TASKS SET STATUS = 'PROCESSING', NEXT_PROCESS_TIME = #{newNextProcessTime}, LAST_UPDATE_TIME = CURRENT_TIMESTAMP() WHERE ID IN ({idList})";

    Future<Task<String>> insert(SqlConnection sqlConnection, String queueName, String refNumber, String payload, Duration processDelay) {
        return SqlTemplate.forUpdate(sqlConnection, SQL_INSERT)
                .execute(Map.of(
                        "queueName", queueName,
                        "payload", payload,
                        "refNumber", refNumber,
                        "nextProcessTime", ZonedDateTime.now().plus(processDelay)))
                .map(sqlResult -> new Task<>(sqlResult.property(JDBCPool.GENERATED_KEYS).getLong(0), payload))
                .onSuccess(task -> log.info("[{}]Task inserted, refNumber:{}, taskId:{}, nextProcessDelay:{}",
                        queueName, refNumber, task.getId(), processDelay))
                .onFailure(err -> log.info("[{}]Fail to insert task, refNumber:{}", queueName, refNumber, err));
    }

    Future<Integer> delete(SqlConnection sqlConnection, long taskId) {
        long start = System.currentTimeMillis();
        return SqlTemplate.forUpdate(sqlConnection, SQL_DELETE)
                .execute(Map.of("id", taskId))
                .map(SqlResult::rowCount)
                .map(deleteCount -> {
                    if (0 == deleteCount) {
                        throw new IllegalStateException(String.format("[taskId:%s] deleted count is 0. Maybe already deleted by another process.", taskId));
                    } else {
                        return deleteCount;
                    }
                })
                .onSuccess(sqlResult -> log.debug("[taskId:{}] task deleted. Time:{}ms", taskId, System.currentTimeMillis() - start))
                .onFailure(err -> log.error("[taskId:{}] fail to delete task. Time:{}ms", taskId, System.currentTimeMillis() - start, err));
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
                        throw new IllegalStateException(String.format("[taskId:%s] updateStatus to [%s] count is 0. Expected: 1", taskId, status));
                    } else {
                        return updateCount;
                    }
                })
                .onSuccess(sqlResult -> log.debug("[taskId:{}] task status updated to [{}]. Time:{}ms", taskId, status, System.currentTimeMillis() - start))
                .onFailure(err -> log.error("[taskId:{}] fail to update status to [{}]. Time:{}ms", taskId, status, System.currentTimeMillis() - start, err));
    }

    Future<List<Task<String>>> checkout(SqlConnection sqlConnection, String queueName, int batchSize, Duration nextProcessDelay) {
        var taskList = selectTasks(sqlConnection, queueName, batchSize);
        return taskList
                .compose(records -> updateNextProcessTime(sqlConnection, records.stream().map(Task::getId).collect(Collectors.toList()), nextProcessDelay))
                .map(records -> taskList.result())
                .onFailure(err -> log.error("[{}] Failed to check out tasks.", queueName, err));
    }


    private Future<List<Task<String>>> selectTasks(SqlConnection sqlConnection, String queueName, int batchSize) {
        long start = System.currentTimeMillis();
        return SqlTemplate.forQuery(sqlConnection, SQL_SELECT_FOR_UPDATE)
                .execute(Map.of("queueName", queueName, "batchSize", batchSize))
                .onFailure(err -> log.error("[{}] selectTasks - failed, time:{}ms", queueName, System.currentTimeMillis() - start, err))
                .map(rows -> {
                    List<Task<String>> records = new ArrayList<>();
                    rows.forEach(row -> records.add(new Task<>(
                            row.getLong("ID"),
                            row.getString("PAYLOAD")
                    )));
                    log.debug("[{}] selectTasks - select count (for update):{}, time:{}ms", queueName, records.size(), System.currentTimeMillis() - start);
                    return records;
                });
    }

    Future<Integer> updateNextProcessTime(SqlConnection sqlConnection, List<Long> taskIdList, Duration nextProcessDelay) {
        long start = System.currentTimeMillis();
        Future<Integer> future;
        if (taskIdList.isEmpty()) {
            future = Future.succeededFuture(0);
        } else {
            String idValues = taskIdList.stream().map(String::valueOf).collect(Collectors.joining(","));
            String sql = SQL_UPDATE_NEXT_PROCESS.replace("{idList}", idValues);
            ZonedDateTime newNextProcessTime = ZonedDateTime.now().plusSeconds(nextProcessDelay.getSeconds());
            future = SqlTemplate.forUpdate(sqlConnection, sql)
                    .execute(Map.of("newNextProcessTime", newNextProcessTime))
                    .map(SqlResult::rowCount)
                    .map(updateCount -> {
                        if (0 == updateCount) {
                            throw new IllegalStateException(String.format("update next process timme: count is 0. Expected: %d, taskIDs: %s ", taskIdList.size(), idValues));
                        } else {
                            return updateCount;
                        }
                    });
        }
        return future
                .onFailure(err -> log.error("{} updateNextProcessTime - failed, time:{}ms", taskIdList, System.currentTimeMillis() - start, err))
                .onSuccess(updateCount -> log.debug("{} updateNextProcessTime - update count:{}, time:{}ms", taskIdList, updateCount, System.currentTimeMillis() - start));
    }

}
