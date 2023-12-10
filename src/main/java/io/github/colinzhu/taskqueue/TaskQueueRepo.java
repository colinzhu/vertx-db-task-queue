package io.github.colinzhu.taskqueue;

import io.vertx.core.Future;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.SqlResult;
import io.vertx.sqlclient.templates.SqlTemplate;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.github.colinzhu.taskqueue.TaskStatus.CREATED;

@Slf4j
class TaskQueueRepo {
    private static final String SQL_INSERT = "INSERT INTO TASKS (QUEUE_NAME, STATUS, ATTEMPT, PAYLOAD, REFERENCE_NUMBER, CREATE_TIME, NEXT_PROCESS_TIME, LAST_UPDATE_TIME) VALUES (#{queueName}, 'CREATED', 0, #{payload}, #{refNumber}, #{createTime}, #{nextProcessTime}, #{lastUpdateTime})";
    private static final String SQL_FINISH_DELETE = "DELETE TASKS WHERE ID = #{id} AND STATUS = 'PROCESSING'"; // only delete status in PROCESSING, in case updated by other already
    private static final String SQL_UPDATE_STATUS_FROM_WITH_RESULT_NULL = "UPDATE TASKS SET STATUS = #{newStatus}, LAST_UPDATE_TIME = #{now}, PROCESS_RESULT = NULL WHERE ID = #{id} and STATUS = #{oriStatus}";
    private static final String SQL_UPDATE_STATUS_FROM_WITH_RESULT = "UPDATE TASKS SET STATUS = #{newStatus}, LAST_UPDATE_TIME = #{now}, PROCESS_RESULT = #{processResult} WHERE ID = #{id} and STATUS = #{oriStatus}";
    private static final String SQL_CHECK_OUT_STEP1_SELECT = "SELECT * FROM TASKS WHERE ID IN (SELECT ID FROM TASKS WHERE STATUS IN ('CREATED','PROCESSING') AND QUEUE_NAME = #{queueName} AND NEXT_PROCESS_TIME <= #{now} ORDER BY NEXT_PROCESS_TIME FETCH FIRST #{batchSize} ROWS ONLY) AND STATUS IN ('CREATED','PROCESSING') AND NEXT_PROCESS_TIME <= #{now} FOR UPDATE SKIP LOCKED";
    private static final String SQL_CHECK_OUT_STEP2_UPDATE = "UPDATE TASKS SET ATTEMPT = ATTEMPT + 1, STATUS = 'PROCESSING', NEXT_PROCESS_TIME = #{newNextProcessTime}, LAST_UPDATE_TIME = #{now} WHERE ID IN ({idList})";
    private static final String SQL_CHECK_OUT_2_STEP1 = "UPDATE TASKS SET ATTEMPT = ATTEMPT + 1, STATUS = 'PROCESSING', POLLER_INSTANCE = #{pollerInstance}, NEXT_PROCESS_TIME = #{newNextProcessTime}, LAST_UPDATE_TIME = #{now} WHERE (ID, LAST_UPDATE_TIME) IN ((SELECT ID, LAST_UPDATE_TIME FROM TASKS WHERE STATUS IN ('CREATED','PROCESSING') AND QUEUE_NAME = #{queueName} AND NEXT_PROCESS_TIME <= #{now} ORDER BY NEXT_PROCESS_TIME FETCH FIRST #{batchSize} ROWS ONLY)) AND NEXT_PROCESS_TIME <= #{now}";
    private static final String SQL_CHECK_OUT_2_STEP2 = "SELECT * FROM TASKS WHERE STATUS = 'PROCESSING' AND QUEUE_NAME = #{queueName} AND POLLER_INSTANCE = #{pollerInstance} AND NEXT_PROCESS_TIME > #{now}";
    private static final String SQL_RE_ENQUEUE_WITH_RESULT_NULL = "UPDATE TASKS SET STATUS = 'CREATED', POLLER_INSTANCE = NULL, NEXT_PROCESS_TIME = #{newNextProcessTime}, LAST_UPDATE_TIME = #{now}, PROCESS_RESULT = NULL WHERE ID = #{id} AND STATUS = 'PROCESSING'";
    private static final String SQL_RE_ENQUEUE_WITH_RESULT = "UPDATE TASKS SET STATUS = 'CREATED', POLLER_INSTANCE = NULL, NEXT_PROCESS_TIME = #{newNextProcessTime}, LAST_UPDATE_TIME = #{now}, PROCESS_RESULT = #{processResult} WHERE ID = #{id} AND STATUS = 'PROCESSING'";

    Future<TaskEntity> insert(SqlConnection sqlConnection, String queueName, String refNumber, String payload, Duration processDelay) {
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

    Future<Integer> completeDelete(SqlConnection sqlConnection, long taskId) {
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

    Future<Integer> updateStatusFromWithResult(SqlConnection sqlConnection, long taskId, TaskStatus oriStatus, TaskStatus newStatus, String processResult) {
        long start = System.currentTimeMillis();
        String truncatedResult = TaskQueueUtils.truncateToUtf8ByteLength(processResult, 4000);
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

    Future<List<TaskEntity>> checkout(SqlConnection sqlConnection, String queueName, int batchSize, Duration deadline) {
        var taskList = checkoutSelect(sqlConnection, queueName, batchSize);
        return taskList
                .compose(records -> checkoutUpdate(sqlConnection, records.stream().map(TaskEntity::getId).collect(Collectors.toList()), deadline))
                .map(records -> taskList.result())
                .onFailure(err -> log.error("task checkout failed: queue={}", queueName, err));
    }

    private Future<List<TaskEntity>> checkoutSelect(SqlConnection sqlConnection, String queueName, int batchSize) {
        long start = System.currentTimeMillis();
        return SqlTemplate.forQuery(sqlConnection, SQL_CHECK_OUT_STEP1_SELECT)
                .execute(Map.of("now", OffsetDateTime.now(), "queueName", queueName, "batchSize", batchSize))
                .onFailure(err -> log.error("task checkoutSelect failed: queue={}, time={}ms", queueName, System.currentTimeMillis() - start, err))
                .map(rows -> {
                    List<TaskEntity> records = new ArrayList<>();
                    rows.forEach(row -> records.add(mapRowToTaskEntityForCheckout(row)));
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
            String sql = SQL_CHECK_OUT_STEP2_UPDATE.replace("{idList}", idValues);
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

    /**
     * <p>The `checkout2` method is designed to fetch a batch of tasks from the database without using row-level locks. This is achieved by using two separate SQL statements in two different connections, not in one transaction.</p>
     * <p>The first SQL statement (`checkout2step1update`) updates the status of the tasks and the second statement (`checkout2step2select`) selects the updated tasks.</p>
     * <p>The `checkout2step1update` SQL statement updates the status of the tasks to 'PROCESSING' and sets the `POLLER_INSTANCE` to the current poller instance. It only updates tasks that are in 'CREATED' or 'PROCESSING' status, whose `NEXT_PROCESS_TIME` is less than or equal to the current time, and only updates up to `batchSize` number of tasks.</p>
     * <p>The `checkout2step2select` SQL statement then selects the tasks that are in 'PROCESSING' status, whose `QUEUE_NAME` matches the given queue name, and whose `POLLER_INSTANCE` matches the current poller instance.</p>
     * <p>In a multi-instance scenario, it is possible that two instances execute the `checkout2step1update` statement at the same time and update the same tasks. However, since each instance sets the `POLLER_INSTANCE` to its own instance identifier, when they execute the `checkout2step2select` statement, they will only select the tasks that they themselves have updated. Therefore, it should not be possible for the same tasks to be fetched by more than one instance.</p>
     * <p>Exception case: The checkout2step1update statement updates the NEXT_PROCESS_TIME to be less than or equal to the current time. So, if an application instance crashes or is stopped after executing the checkout2step1update statement but before executing the checkout2step2select statement, these tasks will not be selected and processed until the application instance restarts and executes the checkout2step2select statement. However, in the meantime, other running instances of the application can execute the checkout2step1update statement and fetch these idled records because their NEXT_PROCESS_TIME is in the past. So, while there might be a delay in processing these tasks, they will not be left unprocessed indefinitely.</p>
     */
    Future<List<TaskEntity>> checkout2(JDBCPool pool, String queueName, int batchSize, Duration nextProcessDelay, String pollerInstance) {
        // 'update' and 'select' are in 2 different connections, not in one transaction
        var step1updateCount = pool.withConnection(sqlConnection -> checkout2step1update(sqlConnection, queueName, batchSize, nextProcessDelay, pollerInstance));
        return step1updateCount
                .compose(count -> {
                    if (count > 0) {
                        return pool.withConnection(sqlConnection -> checkout2step2select(sqlConnection, queueName, pollerInstance));
                    } else {
                        return Future.succeededFuture(new ArrayList<>());
                    }
                }); // the caller has error log, so doesn't print error log here
    }

    private Future<Integer> checkout2step1update(SqlConnection sqlConnection, String queueName, int batchSize, Duration nextProcessDelay, String pollerInstance) {
        long start = System.currentTimeMillis();
        OffsetDateTime newNextProcessTime = OffsetDateTime.now().plusSeconds(nextProcessDelay.getSeconds());
        return SqlTemplate.forUpdate(sqlConnection, SQL_CHECK_OUT_2_STEP1)
                .execute(Map.of("now", OffsetDateTime.now(), "queueName", queueName, "batchSize", batchSize, "newNextProcessTime", newNextProcessTime, "pollerInstance", pollerInstance))
                .map(SqlResult::rowCount)
                .onSuccess(count -> log.info("tasks checkout2step1update success, queue={}, pollerInstance={}, step1updateCount={}, time={}ms", queueName, pollerInstance, count, System.currentTimeMillis() - start))
                .onFailure(err -> log.error("tasks checkout2step1update failed, queue={}, pollerInstance={}, time={}ms", queueName, pollerInstance, System.currentTimeMillis() - start, err));
    }

    private Future<List<TaskEntity>> checkout2step2select(SqlConnection sqlConnection, String queueName, String pollerInstance) {
        long start = System.currentTimeMillis();
        return SqlTemplate.forQuery(sqlConnection, SQL_CHECK_OUT_2_STEP2)
                .execute(Map.of("queueName", queueName, "pollerInstance", pollerInstance, "now", OffsetDateTime.now()))
                .onFailure(err -> log.error("tasks checkout2step2select failed, queue={}, pollerInstance={}, time={}ms", queueName, pollerInstance, System.currentTimeMillis() - start, err))
                .map(rows -> {
                    List<TaskEntity> records = new ArrayList<>();
                    rows.forEach(row -> records.add(mapRowToTaskEntityForCheckout(row)));
                    return records;
                })
                .onSuccess(list -> log.info("tasks checkout2step2select success, queue={}, pollerInstance={}, step2selectCount={}, time={}ms", queueName, pollerInstance, list.size(), System.currentTimeMillis() - start));
    }

    Future<Integer> reenqueue(SqlConnection sqlConnection, Long taskId, Duration nextProcessDelay, String processResult) {
        long start = System.currentTimeMillis();
        OffsetDateTime now = OffsetDateTime.now();
        OffsetDateTime newNextProcessTime = now.plusSeconds(nextProcessDelay.getSeconds());

        String truncatedResult = TaskQueueUtils.truncateToUtf8ByteLength(processResult, 4000);
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

    private static TaskEntity mapRowToTaskEntityForCheckout(Row row) {
        return TaskEntity.builder()
                .id(row.getLong("ID"))
                .referenceNumber(row.getString("REFERENCE_NUMBER"))
                .queueName(row.getString("QUEUE_NAME"))
                .status(TaskStatus.valueOf(row.getString("STATUS")))
                .attempt(row.getLong("ATTEMPT"))
                .createTime(row.getOffsetDateTime("CREATE_TIME"))
                .nextProcessTime(row.getOffsetDateTime("NEXT_PROCESS_TIME"))
                .lastUpdateTime(row.getOffsetDateTime("LAST_UPDATE_TIME"))
                .payload(row.getString("PAYLOAD"))
                .build();
    }

}
