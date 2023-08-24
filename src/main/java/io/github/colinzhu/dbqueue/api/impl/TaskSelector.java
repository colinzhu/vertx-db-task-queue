package io.github.colinzhu.dbqueue.api.impl;

import io.github.colinzhu.dbqueue.internal.TaskRecord;
import io.vertx.core.Future;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.templates.SqlTemplate;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

@Slf4j
@RequiredArgsConstructor
@Accessors(fluent = true)
public class TaskSelector implements Supplier<Future<List<TaskRecord>>> {
    private final JDBCPool pool;
    private final String queueName;
    private int batchSize = 5;
    private Duration nextProcessDelay = Duration.ofMinutes(1);

    @Override
    public Future<List<TaskRecord>> get() {
        log.info("Start to fetch tasks - queueName:{}, batchSize:{}, nextProcessDelay:{}", queueName, batchSize, nextProcessDelay);
        String sql = "SELECT * FROM TASKS WHERE QUEUE_NAME = #{queueName} AND NEXT_PROCESS_TIME <= CURRENT_TIMESTAMP() ORDER BY NEXT_PROCESS_TIME, ID FETCH FIRST #{batchSize} ROWS ONLY FOR UPDATE";
        return pool.withTransaction(sqlConnection -> {
            Future<List<TaskRecord>> recordsFuture = SqlTemplate.forQuery(sqlConnection, sql)
                    .execute(Map.of("queueName", queueName, "batchSize", batchSize))
                    .map(rows -> {
                        List<TaskRecord> records = new ArrayList<>();
                        rows.forEach(row -> records.add(new TaskRecord(
                                row.getLong("ID"),
                                row.getString("QUEUE_NAME"),
                                row.getString("STATUS"),
                                row.getString("PAYLOAD"),
                                row.getString("REFERENCE_NUMBER"),
                                row.getOffsetDateTime("CREATE_TIME").toZonedDateTime(),
                                row.getOffsetDateTime("NEXT_PROCESS_TIME").toZonedDateTime(),
                                row.getOffsetDateTime("LAST_UPDATE_TIME").toZonedDateTime()
                        )));
                        log.info("Number of tasks selected for update:{}", records.size());
                        return records;
                    });
            return recordsFuture.compose(records -> {
                        if (records.isEmpty()) {
                            return Future.succeededFuture();
                        } else {
                            String sqlUpdate = "UPDATE TASKS SET NEXT_PROCESS_TIME = #{newNextProcessTime} WHERE ID = #{id}";
                            return SqlTemplate.forUpdate(sqlConnection, sqlUpdate)
                                    .execute(Map.of("id", records.get(0).getId(), "newNextProcessTime", ZonedDateTime.now().plusSeconds(nextProcessDelay.getSeconds())));
                        }
                    })
                    .map(item -> {
                        log.info("Number of tasks selected for update and committed:{}", recordsFuture.result().size());
                        return recordsFuture.result();
                    });
        });
    }

    @Override
    public String toString() {
        return "TaskBatchSupplier{" +
                "queueName='" + queueName + '\'' +
                ", batchSize=" + batchSize +
                '}';
    }
}
