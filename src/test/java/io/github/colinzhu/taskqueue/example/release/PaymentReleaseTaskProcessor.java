package io.github.colinzhu.taskqueue.example.release;

import io.github.colinzhu.taskqueue.Task;
import io.github.colinzhu.taskqueue.TaskQueueService;
import io.github.colinzhu.taskqueue.example.Payment;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.SqlConnection;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.function.Function;
import java.util.random.RandomGenerator;

@Slf4j
@RequiredArgsConstructor
public class PaymentReleaseTaskProcessor implements Function<Task<Payment>, Future<?>> {
    private final Vertx vertx;
    private final JDBCPool pool;
    private final TaskQueueService taskQueueService;

    @Override
    public Future<Integer> apply(Task<Payment> task) {
        return doSomething(task)
                .compose(payment -> pool.withTransaction(conn -> persistChanges(conn, payment, task)));
    }

    private Future<Integer> persistChanges(SqlConnection txn, Payment payment, Task<Payment> task) {
        return txn.query("UPDATE PAYMENT SET STATUS = 'RELEASED' WHERE ID = " + payment.getId())
                .execute()
                .compose(res -> {
                    if (task.getAttempt() >= 1) {
                        return taskQueueService.complete(txn, task.getId());
                    } else {
                        return taskQueueService.reenqueue(txn, task.getId(), Duration.ofSeconds(task.getAttempt()));
                    }
                });
    }

    // do some blocking task OUTSIDE the DB transaction, e.g. call HTTP API
    private Future<Payment> doSomething(Task<Payment> task) {
        Promise<Payment> promise = Promise.promise();
        vertx.setTimer(RandomGenerator.getDefault().nextInt(499, 500), id -> {
            log.info("[taskId:{}] doSomething release completed. Attempt={}. Payload:{}", task.getId(), task.getAttempt(), task.getPayload());
            promise.complete(task.getPayload());
        });
        return promise.future();
    }
}
