package io.github.colinzhu.taskqueue;

import io.vertx.core.Future;
import io.vertx.sqlclient.SqlConnection;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Set;

@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
class TaskQueueSupportServiceImpl implements TaskQueueSupportService {
    private final TaskQueueSupportRepo supportRepo;
    private static final TaskQueueSupportServiceImpl instance = new TaskQueueSupportServiceImpl(TaskQueueSupportRepo.getInstance());

    static TaskQueueSupportServiceImpl getInstance() {
        return instance;
    }

    @Override
    public Future<Integer> reenqueueFromError(SqlConnection sqlConnection, Set<Long> taskIds) {
        return supportRepo.reenqueueFromError(sqlConnection, taskIds);
    }

    @Override
    public Future<Integer> markPoison(SqlConnection sqlConnection, Set<Long> taskIds) {
        return supportRepo.updateStatusFromToBatch(sqlConnection, taskIds, "ERROR", "POISON");
    }

    @Override
    public Future<Integer> poisonToError(SqlConnection sqlConnection, Set<Long> taskIds) {
        return supportRepo.updateStatusFromToBatch(sqlConnection, taskIds, "POISON", "ERROR");
    }

    @Override
    public Future<Integer> housekeepPoison(SqlConnection sqlConnection, Set<Long> taskIds, OffsetDateTime createTimeBefore) {
        return supportRepo.housekeepPoison(sqlConnection, taskIds, createTimeBefore);
    }

    @Override
    public Future<List<?>> searchByQueueNameAndStatus(SqlConnection sqlConnection, String queueName, String status, int batchSize) {
        return supportRepo.searchByQueueNameAndStatus(sqlConnection, queueName, status, batchSize).compose(Future::succeededFuture);
    }

    @Override
    public Future<List<?>> countGroupByQueueNameAndStatus(SqlConnection sqlConnection) {
        return supportRepo.countGroupByQueueNameAndStatus(sqlConnection).compose(Future::succeededFuture);
    }

}
