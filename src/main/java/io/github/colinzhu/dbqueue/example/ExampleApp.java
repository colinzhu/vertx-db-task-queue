package io.github.colinzhu.dbqueue.example;

import io.github.colinzhu.dbqueue.api.TaskProcessor;
import io.github.colinzhu.dbqueue.api.Task;
import io.github.colinzhu.dbqueue.api.TaskProcessResult;
import io.github.colinzhu.dbqueue.api.impl.TaskSelector;
import io.github.colinzhu.dbqueue.api.impl.QueuePollingTaskProcessor;
import io.github.colinzhu.dbqueue.api.impl.QueueTaskManager;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.jdbcclient.JDBCPool;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.function.Function;

@Slf4j
public class ExampleApp {
    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();

        final JsonObject config = new JsonObject()
                .put("url", "jdbc:h2:~/dev/git/db-queue-vertx/example-db/example-db;DEFAULT_NULL_ORDERING=HIGH")
                //.put("url", "jdbc:h2:tcp://127.0.1.1:9092/example-db")
                .put("driver_class", "org.h2.Driver")
                .put("datasourceName", "example-db")
                .put("user", "sa")
                .put("password", "sa")
                .put("max_pool_size", 100);

        JDBCPool pool = JDBCPool.pool(vertx, config);

        QueueTaskManager queueTaskManager = new QueueTaskManager(pool);
        queueTaskManager.enqueue("Q1", "test1", Duration.ofMinutes(0));

        TaskSelector taskSelector = new TaskSelector(pool, "Q1");
        Function<Task, Future<TaskProcessResult>> taskProcessor = task -> {
            System.out.println(task);
            return Future.succeededFuture(TaskProcessResult.success());
        };
        TaskProcessor poller = new QueuePollingTaskProcessor(vertx, taskSelector, taskProcessor);
        poller.process();
    }
}
