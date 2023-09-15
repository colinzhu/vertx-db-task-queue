package io.github.colinzhu.taskqueue;

import io.vertx.core.Future;
import io.vertx.sqlclient.SqlConnection;

import java.util.Set;

public interface TaskQueueSupportService {
    static TaskQueueSupportService getInstance() {
        return TaskQueueSupportServiceImpl.getInstance();
    }

    Future<Integer> reprocessErrorTasks(SqlConnection sqlConnection, Set<Long> taskIds);
}
