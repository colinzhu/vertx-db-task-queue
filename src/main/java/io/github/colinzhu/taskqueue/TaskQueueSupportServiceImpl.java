package io.github.colinzhu.taskqueue;

import io.vertx.core.Future;
import io.vertx.sqlclient.SqlConnection;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Set;

@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
class TaskQueueSupportServiceImpl implements TaskQueueSupportService {
    private final TaskEntityRepo taskEntityRepo;
    private static final TaskQueueSupportServiceImpl instance = new TaskQueueSupportServiceImpl(TaskEntityRepo.getInstance());

    static TaskQueueSupportServiceImpl getInstance() {
        return instance;
    }

    @Override
    public Future<Integer> reenqueueErrorTasks(SqlConnection sqlConnection, Set<Long> taskIds) {
        return taskEntityRepo.reenqueueErrorTasks(sqlConnection, taskIds);
    }

    @Override
    public Future<List<?>> searchByQueueNameAndStatus(SqlConnection sqlConnection, String queueName, String status, int batchSize) {
        return taskEntityRepo.searchByQueueNameAndStatus(sqlConnection, queueName, status, batchSize).compose(Future::succeededFuture);
    }

    @Override
    public Future<List<?>> countGroupByQueueNameAndStatus(SqlConnection sqlConnection) {
        return taskEntityRepo.countGroupByQueueNameAndStatus(sqlConnection).compose(Future::succeededFuture);
    }

}
