package io.github.colinzhu.taskqueue.example;

import io.github.colinzhu.taskqueue.Task;
import io.github.colinzhu.taskqueue.TaskQueueService;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.SqlConnection;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.function.Function;
import java.util.random.RandomGenerator;

@Slf4j
@RequiredArgsConstructor
public class PaymentCheckTaskProcessor implements Function<Task<Payment>, Future<?>>, Handler<Message<Task<Payment>>> {
    private final Vertx vertx;
    private final JDBCPool pool;
    private final TaskQueueService taskQueueService;

    @Override
    public Future<?> apply(Task<Payment> task) {
        return doSomething(task)
                .compose(payment -> {
                    if (RandomGenerator.getDefault().nextInt(1, 10) == 7) {
                        return Future.failedFuture("Simulate error e.g. after retry still fail case.");
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
                    if (task.getAttempt() >= 1) {
                        return taskQueueService.finish(txn, task.getId())
                                .compose(any -> taskQueueService.enqueue(txn,"payment.release", "REF_" + task.getPayload().getId(), task.getPayload()))
                                .compose(any -> Future.succeededFuture());
                    } else {
                        return taskQueueService.reenqueue(txn, task.getId(), Duration.ofSeconds(task.getAttempt()));
                    }
                });
    }

    // do some blocking task OUTSIDE the DB transaction, e.g. call HTTP API
    private Future<Payment> doSomething(Task<Payment> task) {
        Promise<Payment> promise = Promise.promise();
        vertx.setTimer(RandomGenerator.getDefault().nextInt(100, 500), id -> {
            log.info("[taskId:{}] doSomething check completed. Attempt={}. Payload:{}", task.getId(), task.getAttempt(), task.getPayload());
            promise.complete(task.getPayload());
        });
        return promise.future();
    }


    @Override
    public void handle(Message<Task<Payment>> message) {
        log.info("Task received from event bus, queueName={}, taskId={}, referenceNumber={}", message.address(), message.body().getId(), message.body().getPayload().getId());
        apply(message.body()).recover(err -> {
            log.error("Task from event bus handle failed, queueName={}, taskId={}, referenceNumber={}", "payment.check", message.body().getId(), message.body().getPayload().getId(), err);
            return pool.withConnection(conn -> taskQueueService.fail(conn, message.body().getId()).compose(count -> Future.succeededFuture())); // for recover to mark the task as ERROR, it needs to be in a separate connection
        });
    }
}
