package io.github.colinzhu.dbqueue.api.impl;

import io.github.colinzhu.dbqueue.api.Task;
import io.github.colinzhu.dbqueue.api.TaskPoller;
import io.github.colinzhu.dbqueue.api.TaskProcessResult;
import io.github.colinzhu.dbqueue.internal.TaskRecord;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.templates.SqlTemplate;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
@Accessors(fluent = true)
public class TaskBatchSupplier implements Supplier<Future<List<TaskRecord>>> {
    private final JDBCPool pool;
    private final String queueName;
    private int batchSize = 5;
    private Duration nextProcessDelay = Duration.ofMinutes(1);

    @Override
    public Future<List<TaskRecord>> get() {
        log.info("queueName:{}, batchSize:{}, nextProcessDelay:{}", queueName, batchSize, nextProcessDelay);
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
                    .map(item -> recordsFuture.result());
        });
    }

}
