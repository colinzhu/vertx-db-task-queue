package io.github.colinzhu.taskqueue;

import ch.qos.logback.classic.Level;
import io.github.colinzhu.taskqueue.dispatch.TaskDispatchConfig;
import io.github.colinzhu.taskqueue.enqueue.TaskEnqueueService;
import io.github.colinzhu.taskqueue.example.Payment;
import io.github.colinzhu.taskqueue.process.TaskProcessService;
import io.github.colinzhu.taskqueue.process.TaskProcessor;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.OffsetDateTime;
import java.util.function.Supplier;

import static io.github.colinzhu.taskqueue.TaskQueueTestUtils.*;
import static org.slf4j.Logger.ROOT_LOGGER_NAME;

@ExtendWith(VertxExtension.class)
@Slf4j
class TaskQueueErrorTest {
    private static JDBCPool pool;
    private static Supplier<JDBCPool> poolSupplier;
    private static TaskEnqueueService taskEnqueueService;
    private static TaskProcessService taskProcessService;

    @BeforeAll
    static void init(Vertx vertx, VertxTestContext testContext) {
        setLogLevel(ROOT_LOGGER_NAME, Level.INFO);
        setLogLevel("com.mchange.v2.resourcepool.BasicResourcePool", Level.INFO);
        setLogLevel("io.github.colinzhu.taskqueue", Level.DEBUG);
        setLogLevel("io.github.colinzhu.taskqueue.internal.TaskRepo", Level.DEBUG);
        setLogLevel("io.github.colinzhu.taskqueue.dispatch.TaskDispatcher", Level.DEBUG);

        taskEnqueueService = TaskEnqueueService.getInstance(vertx);
        taskProcessService = TaskProcessService.getInstance();
        Database db = Database.get(Database.H2_MEM);
        pool = db.getJdbcPool(vertx);
        db.createTables(pool).onComplete(ar -> testContext.completeNow());
        poolSupplier = () -> db.getJdbcPool(vertx);
    }

    @ParameterizedTest
    @DisplayName("Task process error - changes should be rolled back, tasks should be updated to ERROR")
    @CsvSource({"ERR_IN_TXN", "ERR_BEFORE_TXN"})
    void testTaskProcessingError(String errLocation, Vertx vertx, VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(2);
        Payment payment = new Payment("CREATED", OffsetDateTime.now());

        TaskProcessor<Payment> taskProcessor = createTaskProcessorWithError(errLocation, payment);
        String queueName = getQueueName(errLocation);
        TaskDispatchConfig<Payment> config = createTaskDispatchConfig(queueName);

        String taskRef = "ref-err-" + System.currentTimeMillis() + errLocation;
        deployDispatchAndProcessVerticles(vertx, config, taskProcessor, poolSupplier)
                .compose(any -> savePaymentAndEnqueue(queueName, payment, taskRef))
                .onComplete(testContext.succeeding(any -> verifyTaskError(vertx, testContext, checkpoint, payment, taskRef)));
    }

    private TaskProcessor<Payment> createTaskProcessorWithError(String errLocation, Payment payment) {
        return task -> {
            if ("ERR_BEFORE_TXN".equals(errLocation)) {
                throw new RuntimeException("simulate exception before transaction");
            }
            return pool.withTransaction(conn ->
                    updatePayment(conn, payment)
                            .map(updateCount -> {
                                if ("ERR_IN_TXN".equals(errLocation)) {
                                    throw new RuntimeException("simulate exception within transaction");
                                }
                                return updateCount;
                            })
                            .compose(count -> taskProcessService.complete(conn, task)));
        };
    }

    private Future<Void> savePaymentAndEnqueue(String queueName, Payment payment, String taskRef) {
        return pool.withTransaction(conn -> savePayment(conn, payment).compose(pay -> taskEnqueueService.enqueue(conn, queueName, taskRef, pay)));
    }


    private void verifyTaskError(Vertx vertx, VertxTestContext testContext, Checkpoint checkpoint, Payment payment, String taskRef) {
        vertx.setTimer(1000, id -> {
            retrievePayment(payment.getId(), pool).onComplete(testContext.succeeding(p -> {
                Assertions.assertEquals("CREATED", p.getStatus(), "PAYMENT status should be rolled back / no change");
                checkpoint.flag();
            }));

            retrieveTask(taskRef, pool).onComplete(testContext.succeeding(row -> {
                Assertions.assertEquals("ERROR", row.getString("STATUS"), "task should be updated to ERROR");
                checkpoint.flag();
            }));
        });
    }

}