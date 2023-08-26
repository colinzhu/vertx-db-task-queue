package io.github.colinzhu.dbqueue.api.poller;

import io.github.colinzhu.dbqueue.api.QueueConfig;
import io.github.colinzhu.dbqueue.api.Task;
import io.vertx.core.Future;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.templates.SqlTemplate;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

import java.time.ZonedDateTime;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
@Accessors(fluent = true)
class TaskSelector implements Supplier<Future<List<Task>>> {
    private final JDBCPool pool;
    private final QueueConfig queueConfig;

    private static final String SQL_SELECT = "SELECT * FROM TASKS WHERE QUEUE_NAME = #{queueName} AND NEXT_PROCESS_TIME <= CURRENT_TIMESTAMP() ORDER BY NEXT_PROCESS_TIME, ID FETCH FIRST #{batchSize} ROWS ONLY FOR UPDATE";
    private static final String SQL_UPDATE = "UPDATE TASKS SET NEXT_PROCESS_TIME = #{newNextProcessTime} WHERE ID IN (#{idList})";
    @Override
    public Future<List<Task>> get() {
        String corrId = queueConfig.getQueueName() + "-" + System.currentTimeMillis();
        return pool.withTransaction(sqlConnection ->
                selectTasks(sqlConnection, corrId)
                .compose(records -> updateTasks(sqlConnection, corrId, records)));
    }

    private Future<List<Task>> selectTasks(SqlConnection sqlConnection, String corrId) {
        log.info("selectTasks - [{}], nextProcessDelay:{}", corrId, queueConfig.getNextProcessDelay());
        return SqlTemplate.forQuery(sqlConnection, SQL_SELECT)
                .execute(Map.of("queueName", queueConfig.getQueueName(), "batchSize", queueConfig.getBatchSize()))
                .onFailure(err -> log.error("selectTasks - [{}], failed", err))
                .map(rows -> {
                    List<Task> records = new ArrayList<>();
                    rows.forEach(row -> records.add(new Task(
                            row.getLong("ID"),
//                                row.getString("QUEUE_NAME"),
//                                row.getString("STATUS"),
                            row.getString("PAYLOAD")
//                                row.getString("REFERENCE_NUMBER")
//                                row.getOffsetDateTime("CREATE_TIME").toZonedDateTime(),
//                                row.getOffsetDateTime("NEXT_PROCESS_TIME").toZonedDateTime(),
//                                row.getOffsetDateTime("LAST_UPDATE_TIME").toZonedDateTime()
                    )));
                    log.info("selectTasks - [{}], select count (for update):{}", records.size());
                    return records;
                });
    }

    private Future<List<Task>> updateTasks(SqlConnection sqlConnection, String corrId, List<Task> records) {
        Future<List<Task>> future;
        if (records.isEmpty()) {
            future = Future.succeededFuture(List.of());
        } else {
            System.out.println("************");
            //Set<Long> idValues = records.stream().map(Task::getId).collect(Collectors.toSet());
            Set<Long> idValues = new HashSet<>();
            idValues.add(1L);
            String idKeyList = getInKeyValueMap(Set.of(idValues), "idIn").keySet().stream().sorted().map(k -> "#{" + k + "}").collect(Collectors.joining(","));
            System.out.println(idKeyList);
            String sql = SQL_UPDATE.replace("#{idList}", idKeyList);
            System.out.println(sql);
            ZonedDateTime newNextProcessTime = ZonedDateTime.now().plusSeconds(queueConfig.getNextProcessDelay().getSeconds());
            future = SqlTemplate.forUpdate(sqlConnection, sql)
                    .execute(Map.of("idList", idValues, "newNextProcessTime", newNextProcessTime))
                    .map(records);
        }
        return future.onSuccess(tasks -> log.info("updateTasks - [{}] updated count:{}", corrId, tasks.size()));
    }

    @Override
    public String toString() {
        return "TaskSelector{" +
                "queueConfig=" + queueConfig +
                '}';
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
