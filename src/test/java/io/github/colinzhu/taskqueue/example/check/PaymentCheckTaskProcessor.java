package io.github.colinzhu.taskqueue.example.check;

import io.github.colinzhu.taskqueue.Task;
import io.github.colinzhu.taskqueue.TaskQueueService;
import io.github.colinzhu.taskqueue.example.Payment;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.SqlConnection;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.random.RandomGenerator;

@Slf4j
public class PaymentCheckTaskProcessor implements Function<Task<Payment>, Future<?>> {
    private final Vertx vertx;
    private final TaskQueueService taskQueueService;
    private final JDBCPool pool;

    public PaymentCheckTaskProcessor(Vertx vertx, Supplier<JDBCPool> poolSupplier, TaskQueueService taskQueueService) {
        this.vertx = vertx;
        this.taskQueueService = taskQueueService;
        this.pool = poolSupplier.get();
    }

    @Override
    public Future<?> apply(Task<Payment> task) {
        return doSomething(task)
                .compose(payment -> {
                    if (RandomGenerator.getDefault().nextInt(1, 10) == 7) {
                        return Future.failedFuture("Simulate error e.g. after retry still fail case.");
                    } else if (RandomGenerator.getDefault().nextInt(1, 10) == 8) {
                        throw new RuntimeException("Test runtime exception.");
                    } else {
                        return Future.succeededFuture(payment);
                    }
                })
                .compose(payment -> pool.withTransaction(conn -> persistChanges(conn, payment, task)));
    }

    private Future<?> persistChanges(SqlConnection txn, Payment payment, Task<Payment> task) {
        return txn.query("UPDATE PAYMENT SET STATUS = 'PENDING_RELEASE' WHERE ID = " + payment.getId())
                .execute()
                .compose(res -> {
                    if (task.getAttempt() >= 3 || RandomGenerator.getDefault().nextInt(1, 3) == 2) {
                        return taskQueueService.complete(txn, task.getId(), "test complete result")
                                .compose(any -> taskQueueService.enqueue(txn,"payment.release", "REF_" + task.getPayload().getId(), task.getPayload()))
                                .compose(any -> Future.succeededFuture());
                    } else {
                        return taskQueueService.reenqueue(txn, task.getId(), Duration.ofSeconds(1), "test reenqueue process result " + task.getAttempt());
                    }
                });
    }

    // do some blocking task OUTSIDE the DB transaction, e.g. call HTTP API
    private Future<Payment> doSomething(Task<Payment> task) {
        Promise<Payment> promise = Promise.promise();
        vertx.setTimer(RandomGenerator.getDefault().nextInt(499, 500), id -> {
            log.info("[taskId:{}] doSomething check completed. Attempt={}. Payload:{}", task.getId(), task.getAttempt(), task.getPayload());
            promise.complete(task.getPayload());
        });
        return promise.future();
    }
}
