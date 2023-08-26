package io.github.colinzhu.taskqueue.example;

import io.github.colinzhu.taskqueue.manager.TaskQueueManager;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Tuple;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
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

        router.route("/create/:count").handler(routingContext -> {
            String count = routingContext.pathParam("count");
            if (count != null) {
                for (int i = 0; i < Integer.parseInt(count); i++) {
                    Payment p = new Payment(System.nanoTime(), "CREATED", "B", System.currentTimeMillis());
                    pool.withTransaction(sqlConnection -> {
                        long start = System.currentTimeMillis();
                        return insert(sqlConnection, p)
                                .compose(payment -> TaskQueueManager.taskQueue()
                                        .enqueue(sqlConnection, "QueuePaymentToBeChecked", UUID.randomUUID().toString(), payment.toString(), Duration.ZERO))
                                .onSuccess(event -> log.info("payment created, time: {}ms", System.currentTimeMillis() - start))
                                .onFailure(e -> log.info("error waiting for all", e));
                    });
                }
                routingContext.response().end("create triggered");
            } else {
                routingContext.response().end("ready to create");
            }
        });
        server.requestHandler(router).listen(port).onSuccess(ar -> log.info("create server started at {}", port));
    }


    private Future<Payment> insert(SqlConnection sqlConnection, Payment payment) {
        long start = System.currentTimeMillis();
        return sqlConnection.preparedQuery("insert into PAYMENT (STATUS, INSTANCE, CREATE_TIME) values (?, ?, ?)")
                .execute(Tuple.of(payment.getStatus(), payment.getInstance(), payment.getCreateTime()))
                .onSuccess(rows -> log.info("payment inserted, ID:{} time:{}ms", rows.property(JDBCPool.GENERATED_KEYS).getLong(0), System.currentTimeMillis() - start))
                .map(result -> payment)
                .onFailure(e -> log.error("error inserting", e));
    }

}
