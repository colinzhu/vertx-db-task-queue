package io.github.colinzhu.taskqueue.processing;

import io.github.colinzhu.taskqueue.internal.TaskRepo;
import io.vertx.core.Future;
import io.vertx.sqlclient.SqlConnection;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;

import static io.github.colinzhu.taskqueue.internal.TaskStatus.COMPLETED;
import static io.github.colinzhu.taskqueue.internal.TaskStatus.PROCESSING;

@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
class TaskProcessServiceImpl implements TaskProcessService {
    private final TaskRepo taskRepo = new TaskRepo();

    @Override
    public <T> Future<Integer> complete(SqlConnection sqlConnection, Task<T> task) {
        return complete(sqlConnection, task, null);
    }

    @Override
    public <T> Future<Integer> complete(SqlConnection sqlConnection, Task<T> task, String processResult) {
        return taskRepo.updateStatusFromWithResult(sqlConnection, task.getId(), PROCESSING, COMPLETED, processResult);
    }

    @Override
    public <T> Future<Integer> completeDelete(SqlConnection sqlConnection, Task<T> task) {
        return taskRepo.completeDelete(sqlConnection, task.getId());
    }

    @Override
    public <T> Future<Integer> reenqueue(SqlConnection sqlConnection, Task<T> task, Duration delay) {
        return reenqueue(sqlConnection, task, delay, null);
    }

    @Override
    public <T> Future<Integer> reenqueue(SqlConnection sqlConnection, Task<T> task, Duration delay, String processResult) {
        return taskRepo.reenqueue(sqlConnection, task.getId(), delay, processResult);
        // not sending new task notification to poller, the task will wait for some time before being processed, max noTaskInterval
        // because:
        // 1. usually reenqueue should have a delay
        // 2. there is no direct queue name to send notification
    }
}
