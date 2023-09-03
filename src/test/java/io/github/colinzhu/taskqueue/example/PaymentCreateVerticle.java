package io.github.colinzhu.taskqueue.example;

import io.github.colinzhu.taskqueue.TaskQueueService;
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
                .onSuccess(ar -> log.info("create server started. http://localhost:{}/create/1", port));
    }

    private void handleRouting(RoutingContext routingContext) {
        long start = System.currentTimeMillis();
        int count;
        try {
            count = Integer.parseInt(routingContext.pathParam("count"));
        } catch (RuntimeException e) {
            routingContext.response().end("count must be an integer");
            return;
        }
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            Payment p = new Payment("CREATED", System.currentTimeMillis());
            final int i2 = i;
            futures.add(pool.withTransaction(sqlConnection -> insertPaymentAndTask(i2, p, sqlConnection)));
        }
        Future.join(futures)
                .onSuccess(res -> routingContext.response().end(count + " items created, time: " + (System.currentTimeMillis() - start) + "ms"))
                .onFailure(res -> routingContext.response().end("fail to create items"));
    }

    private Future<?> insertPaymentAndTask(int number, Payment p, SqlConnection sqlConnection) {
        long start = System.currentTimeMillis();
        return insertPayment(sqlConnection, p)
                .compose(payment -> TaskQueueService.taskQueue().enqueue(sqlConnection, "QueuePaymentToBeChecked", UUID.randomUUID().toString(), payment))
                .onSuccess(event -> log.debug("#{} payment and task created, time: {}ms", number, System.currentTimeMillis() - start))
                .onFailure(e -> log.error("error creating payment / task", e));
    }

    private Future<Payment> insertPayment(SqlConnection sqlConnection, Payment payment) {
        long start = System.currentTimeMillis();
        return sqlConnection.preparedQuery("insert into PAYMENT (STATUS, CREATE_TIME) values (?, ?, ?)")
                .execute(Tuple.of(payment.getStatus(), payment.getCreateTime()))
                .onSuccess(rows -> log.debug("payment inserted, ID:{} time:{}ms", rows.property(JDBCPool.GENERATED_KEYS).getLong(0), System.currentTimeMillis() - start))
                .map(result -> payment)
                .onFailure(e -> log.error("error inserting", e));
    }

}
