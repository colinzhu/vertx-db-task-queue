package io.github.colinzhu.taskqueue.example;

import io.github.colinzhu.taskqueue.manager.TaskQueueManager;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Tuple;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Slf4j
@RequiredArgsConstructor
public class PaymentCreateVerticle extends AbstractVerticle {
    private final JDBCPool pool;

    @Override
    public void start() throws Exception {
        super.start();
        startHttpServer();
    }

    private void startHttpServer() {
        int port = 8081;
        HttpServer server = vertx.createHttpServer();
        Router router = Router.router(vertx);
        router.route("/create/:count").handler(this::handleRouting);
        server.requestHandler(router).listen(port)
                .onSuccess(ar -> log.info("create server started at {}", port));
    }

    private void handleRouting(RoutingContext routingContext) {
        long start = System.currentTimeMillis();
        String count = routingContext.pathParam("count");
        if (null == count) {
            routingContext.response().end("count cannot be null");
            return;
        }
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < Integer.parseInt(count); i++) {
            Payment p = new Payment(System.nanoTime(), "CREATED", "B", System.currentTimeMillis());
            final int i2 = i;
            futures.add(pool.withTransaction(sqlConnection -> insertPaymentAndTask(i2, p, sqlConnection)));
        }
        Future.join(futures)
                .onSuccess(res -> routingContext.response().end("all items created, time: " + (System.currentTimeMillis() - start) + "ms"))
                .onFailure(res -> routingContext.response().end("fail to create items"));
    }

    private Future<?> insertPaymentAndTask(int number, Payment p, SqlConnection sqlConnection) {
        long start = System.currentTimeMillis();
        return insertPayment(sqlConnection, p)
                .compose(payment -> TaskQueueManager.taskQueue(vertx).enqueue(sqlConnection, "QueuePaymentToBeChecked", UUID.randomUUID().toString(), payment.toString(), Duration.ofSeconds(5)))
                .onSuccess(event -> log.info("#{} payment and task created, time: {}ms", number, System.currentTimeMillis() - start))
                .onFailure(e -> log.info("error creating payment / task", e));
    }


    private Future<Payment> insertPayment(SqlConnection sqlConnection, Payment payment) {
        long start = System.currentTimeMillis();
        return sqlConnection.preparedQuery("insert into PAYMENT (STATUS, INSTANCE, CREATE_TIME) values (?, ?, ?)")
                .execute(Tuple.of(payment.getStatus(), payment.getInstance(), payment.getCreateTime()))
                .onSuccess(rows -> log.info("payment inserted, ID:{} time:{}ms", rows.property(JDBCPool.GENERATED_KEYS).getLong(0), System.currentTimeMillis() - start))
                .map(result -> payment)
                .onFailure(e -> log.error("error inserting", e));
    }

}
