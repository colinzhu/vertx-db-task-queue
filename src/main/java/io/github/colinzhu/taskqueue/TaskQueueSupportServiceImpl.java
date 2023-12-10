package io.github.colinzhu.taskqueue;

import io.vertx.core.Future;
import io.vertx.sqlclient.SqlConnection;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Set;

import static io.github.colinzhu.taskqueue.TaskStatus.ERROR;
import static io.github.colinzhu.taskqueue.TaskStatus.POISON;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PACKAGE)
class TaskQueueSupportServiceImpl implements TaskQueueSupportService {
    private final TaskQueueSupportRepo supportRepo = new TaskQueueSupportRepo();

    @Override
    public Future<Integer> reenqueueFromError(SqlConnection sqlConnection, Set<Long> taskIds) {
        return supportRepo.reenqueueFromError(sqlConnection, taskIds);
        // not sending new task notification to poller, the task will wait for some time before being processed, max noTaskInterval
        // because:
        // 1. there is no direct queue name to send notification
        // 2. don't get the queue name by id, because the taskIds may belong to different queues
    }

    @Override
    public Future<Integer> markPoison(SqlConnection sqlConnection, Set<Long> taskIds) {
        return supportRepo.updateStatusFromToBatch(sqlConnection, taskIds, ERROR, POISON);
    }

    @Override
    public Future<Integer> poisonToError(SqlConnection sqlConnection, Set<Long> taskIds) {
        return supportRepo.updateStatusFromToBatch(sqlConnection, taskIds, POISON, ERROR);
    }

    @Override
    public Future<Integer> housekeepPoison(SqlConnection sqlConnection, Set<Long> taskIds, OffsetDateTime createTimeBefore) {
        return supportRepo.housekeepPoison(sqlConnection, taskIds, createTimeBefore);
    }

    @Override
    public Future<List<?>> searchByQueueNameAndStatus(SqlConnection sqlConnection, String queueName, String status, int batchSize) {
        return supportRepo.searchByQueueNameAndStatus(sqlConnection, queueName, TaskStatus.valueOf(status), batchSize).compose(Future::succeededFuture);
    }

    @Override
    public Future<List<?>> countGroupByQueueNameAndStatus(SqlConnection sqlConnection) {
        return supportRepo.countGroupByQueueNameAndStatus(sqlConnection).compose(Future::succeededFuture);
    }

}
