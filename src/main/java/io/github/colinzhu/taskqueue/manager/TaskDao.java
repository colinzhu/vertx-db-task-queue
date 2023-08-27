package io.github.colinzhu.taskqueue.manager;

import io.github.colinzhu.taskqueue.Task;
import io.vertx.core.Future;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.templates.SqlTemplate;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
class TaskDao {
    private static final TaskDao instance = new TaskDao();

    public static TaskDao getInstance() {
        return instance;
    }

    private TaskDao() {
    }
    private static final String SQL_INSERT = "INSERT INTO TASKS (QUEUE_NAME, STATUS, PAYLOAD, REFERENCE_NUMBER, NEXT_PROCESS_TIME) VALUES (#{queueName}, 'NEW', #{payload}, #{refNumber}, #{nextProcessTime})";
    private static final String SQL_DELETE = "DELETE TASKS WHERE ID = #{id}";

    private static final String SQL_SELECT_FOR_UPDATE = "SELECT * FROM TASKS WHERE QUEUE_NAME = #{queueName} AND NEXT_PROCESS_TIME <= CURRENT_TIMESTAMP() ORDER BY NEXT_PROCESS_TIME, ID FETCH FIRST #{batchSize} ROWS ONLY FOR UPDATE";
    private static final String SQL_SELECT_FOR_UPDATE_UPDATE = "UPDATE TASKS SET NEXT_PROCESS_TIME = #{newNextProcessTime} WHERE ID IN (#{idList})";

    Future<Task> insert(SqlConnection sqlConnection, String queueName, String refNumber, String payload, Duration processDelay) {
        return SqlTemplate.forUpdate(sqlConnection, SQL_INSERT)
                .execute(Map.of(
                        "queueName", queueName,
                        "payload", payload,
                        "refNumber", refNumber,
                        "nextProcessTime", ZonedDateTime.now().plus(processDelay)))
                .map(sqlResult -> new Task(sqlResult.property(JDBCPool.GENERATED_KEYS).getLong(0), payload))
                .onSuccess(task -> log.info("[{}]Task inserted, refNumber:{}, taskId:{}, nextProcessDelay:{}",
                        queueName, refNumber, task.getId(), processDelay))
                .onFailure(err -> log.info("[{}]Fail to insert task, refNumber:{}", queueName, refNumber, err));
    }
    Future<?> delete(SqlConnection sqlConnection, long taskId) {
        long start = System.currentTimeMillis();
        return SqlTemplate.forUpdate(sqlConnection, SQL_DELETE)
                .execute(Map.of("id", taskId))
                .onSuccess(sqlResult -> log.info("[taskId:{}] task deleted. Time:{}ms", taskId, System.currentTimeMillis() - start))
                .onFailure(err -> log.error("[taskId:{}] fail to delete task. Time:{}ms", taskId, System.currentTimeMillis() - start, err));
    }

    Future<List<Task>> checkout(SqlConnection sqlConnection, String queueName, int batchSize, Duration nextProcessDelay) {
        return selectTasks(sqlConnection, queueName, batchSize)
                .compose(records -> updateTasks(sqlConnection, records, queueName, nextProcessDelay));
    }


    private Future<List<Task>> selectTasks(SqlConnection sqlConnection, String queueName, int batchSize) {
        long start = System.currentTimeMillis();
        return SqlTemplate.forQuery(sqlConnection, SQL_SELECT_FOR_UPDATE)
                .execute(Map.of("queueName", queueName, "batchSize", batchSize))
                .onFailure(err -> log.error("[{}] selectTasks - failed, time:{}ms", queueName, System.currentTimeMillis() - start, err))
                .map(rows -> {
                    List<Task> records = new ArrayList<>();
                    rows.forEach(row -> records.add(new Task(
                            row.getLong("ID"),
                            row.getString("PAYLOAD")
                    )));
                    log.debug("[{}] selectTasks - select count (for update):{}, time:{}ms", queueName, records.size(), System.currentTimeMillis() - start);
                    return records;
                });
    }

    private Future<List<Task>> updateTasks(SqlConnection sqlConnection, List<Task> records, String queueName, Duration nextProcessDelay) {
        long start = System.currentTimeMillis();
        Future<List<Task>> future;
        if (records.isEmpty()) {
            future = Future.succeededFuture(List.of());
        } else {
            Set<Long> idValues = records.stream().map(Task::getId).collect(Collectors.toSet());
            String idKeyList = getInKeyValueMap(idValues, "idIn").keySet().stream().sorted().map(k -> "#{" + k + "}").collect(Collectors.joining(","));
            String sql = SQL_SELECT_FOR_UPDATE_UPDATE.replace("#{idList}", idKeyList);
            ZonedDateTime newNextProcessTime = ZonedDateTime.now().plusSeconds(nextProcessDelay.getSeconds());
            future = SqlTemplate.forUpdate(sqlConnection, sql)
                    .execute(Map.of("idList", idValues, "newNextProcessTime", newNextProcessTime))
                    .map(records);
        }
        return future
                .onFailure(err -> log.error("[{}] updateTasks - failed, time:{}ms", queueName, System.currentTimeMillis() - start, err))
                .onSuccess(tasks -> log.debug("[{}] updateTasks - updated count:{}, time:{}ms", queueName, tasks.size(), System.currentTimeMillis() - start));
    }

    private <T> Map<String, T> getInKeyValueMap(Set<T> values, String key) {
        SortedSet<T> valueSet = new TreeSet<>(values);
        Map<String, T> templateKeyValueMap = new HashMap<>();
        int i = 0;
        for (T value : valueSet) {
            templateKeyValueMap.put(key + i, value);
            i++;
        }
        return templateKeyValueMap;
    }

}
