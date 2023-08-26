package io.github.colinzhu.taskqueue.example;

import io.vertx.core.Verticle;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.jdbcclient.JDBCPool;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ExampleApp {
    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();
        JDBCPool pool = getJdbcPool(vertx);

        Verticle createVerticle = new PaymentCreateVerticle(pool);
        Verticle consumeVerticle = new PaymentCheckVerticle(pool);

        vertx.deployVerticle(consumeVerticle)
                .compose(any -> vertx.deployVerticle(createVerticle));
    }

    private static JDBCPool getJdbcPool(Vertx vertx) {
        final JsonObject config = new JsonObject()
                //.put("url", "jdbc:h2:~/dev/git/db-queue-vertx/example-db/example-db;DEFAULT_NULL_ORDERING=HIGH")
                .put("url", "jdbc:h2:tcp://127.0.1.1:9092/example-db")
                .put("driver_class", "org.h2.Driver")
                .put("datasourceName", "example-db")
                .put("user", "sa")
                .put("password", "sa")
                .put("max_pool_size", 100);

        // prepare basic component
        return JDBCPool.pool(vertx, config);
    }
}
