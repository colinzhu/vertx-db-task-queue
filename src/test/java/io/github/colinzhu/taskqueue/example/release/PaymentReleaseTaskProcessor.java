package io.github.colinzhu.taskqueue.example.release;

import io.github.colinzhu.taskqueue.enqueue.TaskEnqueueService;
import io.github.colinzhu.taskqueue.processing.Task;
import io.github.colinzhu.taskqueue.example.Payment;
import io.github.colinzhu.taskqueue.processing.TaskProcessService;
import io.github.colinzhu.taskqueue.processing.TaskProcessor;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.SqlConnection;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.function.Supplier;
import java.util.random.RandomGenerator;

@Slf4j
public class PaymentReleaseTaskProcessor implements TaskProcessor<Payment> {
    private final Vertx vertx;
    private final TaskProcessService taskProcessService;
    private final JDBCPool pool;

    public PaymentReleaseTaskProcessor(Vertx vertx,
                                       Supplier<JDBCPool> poolSupplier,
                                       TaskProcessService taskProcessService) {
        this.vertx = vertx;
        this.taskProcessService = taskProcessService;
        this.pool = poolSupplier.get();
    }

    @Override
    public Future<Integer> process(Task<Payment> task) {
        return doSomething(task)
                .recover(err -> Future.succeededFuture(err.getMessage()))
                .compose(res -> pool.withTransaction(conn -> persistChanges(conn, res, task)));
    }

    private Future<Integer> persistChanges(SqlConnection txn, String result, Task<Payment> task) {
        if ("success".equals(result)) {
            return txn.query("UPDATE PAYMENT SET STATUS = 'RELEASED' WHERE ID = " + task.getPayload().getId())
                    .execute()
                    .compose(res -> taskProcessService.complete(txn, task));
        } else { // example to use reenqueue as a retry
            return taskProcessService.reenqueue(txn, task, Duration.ofSeconds(2), "failed to release, retry in 2 sec, err=" + result);
        }
    }

    // do some blocking task OUTSIDE the DB transaction, e.g. call HTTP API
    private Future<String> doSomething(Task<Payment> task) {
        Promise<String> promise = Promise.promise();
        vertx.setTimer(RandomGenerator.getDefault().nextInt(499, 500), id -> {
            if (RandomGenerator.getDefault().nextInt(1, 10) != 7) {
                log.info("[taskId:{}] doSomething release completed. Attempt={}. Payload:{}", task.getId(), task.getAttempt(), task.getPayload());
                promise.complete("success");
            } else {
                log.error("[taskId:{}] doSomething release failed. Attempt={}. Payload:{}", task.getId(), task.getAttempt(), task.getPayload());
                promise.fail(new RuntimeException("simulate failed to release"));
            }
        });
        return promise.future();
    }
}
