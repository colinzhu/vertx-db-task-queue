package io.github.colinzhu.taskqueue.example.create;

import io.github.colinzhu.taskqueue.TaskQueueService;
import io.github.colinzhu.taskqueue.example.Payment;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Tuple;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

@Slf4j
@RequiredArgsConstructor
public class PaymentCreateHandler implements Supplier<Router> {
    private final Vertx vertx;
    private final JDBCPool pool;
    private final TaskQueueService taskQueueService;

    @Override
    public Router get() {
        Router router = Router.router(vertx);
        router.route("/create/:count").handler(this::create);
        return router;
    }

    private void create(RoutingContext routingContext) {
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
            Payment p = new Payment("CREATED", OffsetDateTime.now());
            final int i2 = i;
            futures.add(
                    pool.withTransaction(conn -> insertPayment(conn, p)
                                    .compose(payment -> taskQueueService.enqueue(conn, "payment.check", "REF_" + payment.getId(), payment)))
                            .onSuccess(event -> log.debug("#{} payment and task created, time: {}ms", i2, System.currentTimeMillis() - start))
                            .onFailure(e -> log.error("error creating payment / task", e))
            );
        }
        Future.join(futures)
                .onSuccess(res -> routingContext.response().end(count + " items created, time: " + (System.currentTimeMillis() - start) + "ms"))
                .onFailure(res -> routingContext.response().end("fail to create items"));
    }


    private Future<Payment> insertPayment(SqlConnection sqlConnection, Payment payment) {
        long start = System.currentTimeMillis();
        return sqlConnection.preparedQuery("insert into PAYMENT (STATUS, CREATE_TIME) values (?, ?)")
                .execute(Tuple.of(payment.getStatus(), payment.getCreateTime()))
                .map(rows -> {
                    Long id = rows.property(JDBCPool.GENERATED_KEYS).getLong(0);
                    payment.setId(id);
                    return payment;
                })
                .onSuccess(rows -> log.debug("payment inserted, ID:{} time:{}ms", payment.getId(), System.currentTimeMillis() - start))
                .onFailure(e -> log.error("error inserting", e));
    }

}
