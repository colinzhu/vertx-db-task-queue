package io.github.colinzhu.taskqueue;

import io.vertx.core.Future;
import io.vertx.sqlclient.SqlConnection;

import java.util.List;
import java.util.Set;

public interface TaskQueueSupportService {
    static TaskQueueSupportService getInstance() {
        return TaskQueueSupportServiceImpl.getInstance();
    }

    Future<Integer> reenqueueErrorTasks(SqlConnection sqlConnection, Set<Long> taskIds);
    Future<Integer> markPoison(SqlConnection sqlConnection, Set<Long> taskIds);
    Future<Integer> poisonToError(SqlConnection sqlConnection, Set<Long> taskIds);
    Future<List<?>> searchByQueueNameAndStatus(SqlConnection sqlConnection, String queueName, String status, int batchSize);

    Future<List<?>> countGroupByQueueNameAndStatus(SqlConnection sqlConnection);
}
