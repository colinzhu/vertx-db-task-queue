package io.github.colinzhu.dbqueue.example;

import io.github.colinzhu.dbqueue.api.TaskPoller;
import io.github.colinzhu.dbqueue.api.Task;
import io.github.colinzhu.dbqueue.api.TaskProcessResult;
import io.github.colinzhu.dbqueue.api.impl.TaskBatchSupplier;
import io.github.colinzhu.dbqueue.api.impl.TaskPollerImpl;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.jdbcclient.JDBCPool;

import java.util.function.Function;

public class ExampleApp {
    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();

        final JsonObject config = new JsonObject()
                .put("url", "jdbc:h2:tcp://127.0.1.1:9092/example-db")
                .put("driver_class", "org.h2.Driver")
                .put("datasourceName", "example-db")
                .put("user", "sa")
                .put("password", "sa")
                .put("max_pool_size", 100);
        JDBCPool pool = JDBCPool.pool(vertx, config);
        TaskBatchSupplier taskBatchSupplier = new TaskBatchSupplier(pool, "Q1");
        Function<Task, Future<TaskProcessResult>> taskProcessor = task -> {
            System.out.println(task);
            return Future.succeededFuture(TaskProcessResult.success());
        };
        TaskPoller poller = new TaskPollerImpl(vertx, taskBatchSupplier, taskProcessor);
        poller.start();
    }
}
