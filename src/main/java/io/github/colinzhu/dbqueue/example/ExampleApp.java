package io.github.colinzhu.dbqueue.example;

import io.github.colinzhu.dbqueue.api.PollConfig;
import io.github.colinzhu.dbqueue.api.manager.TaskQueueManager;
import io.github.colinzhu.dbqueue.api.poller.TaskPoller;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Verticle;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.jdbcclient.JDBCPool;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.UUID;

@Slf4j
public class ExampleApp extends AbstractVerticle {
    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();
        Verticle v = new ExampleApp();
        vertx.deployVerticle(v);
    }

    @Override
    public void start() throws Exception {
        JDBCPool pool = getJdbcPool();

        ExampleTaskProcessor taskProcessorQ1 = new ExampleTaskProcessor(pool);
        PollConfig pollConfigQ1 = new PollConfig("Q1", 5, Duration.ofMinutes(10), taskProcessorQ1);
        TaskPoller pollerQ1 = new TaskPoller(vertx, pool, pollConfigQ1); // how often to fetch tasks
        pollerQ1.start();

        // put a task into one queue
        pool.withConnection(sqlConnection ->
                TaskQueueManager.taskQueue().enqueue(sqlConnection, pollConfigQ1.getQueueName(), UUID.randomUUID().toString(), Duration.ofSeconds(1)));
    }

    private JDBCPool getJdbcPool() {
        final JsonObject config = new JsonObject()
                //.put("url", "jdbc:h2:~/dev/git/db-queue-vertx/example-db/example-db;DEFAULT_NULL_ORDERING=HIGH")
                .put("url", "jdbc:h2:tcp://127.0.1.1:9092/example-db")
                .put("driver_class", "org.h2.Driver")
                .put("datasourceName", "example-db")
                .put("user", "sa")
                .put("password", "sa")
                .put("max_pool_size", 100);

        // prepare basic component
        JDBCPool pool = JDBCPool.pool(vertx, config);
        return pool;
    }
}
