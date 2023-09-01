package io.github.colinzhu.taskqueue;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.github.colinzhu.taskqueue.example.Payment;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.SqlResult;
import io.vertx.sqlclient.Tuple;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.function.Function;

import static org.slf4j.Logger.ROOT_LOGGER_NAME;

@ExtendWith(VertxExtension.class)
@Slf4j
class TaskPollerTest {
    private JDBCPool pool;

    @BeforeEach
    void setUp(Vertx vertx, VertxTestContext testContext) {
        setLogLevel(ROOT_LOGGER_NAME, Level.INFO);
        setLogLevel("com.mchange.v2.resourcepool.BasicResourcePool", Level.INFO);
        setLogLevel("io.github.colinzhu.taskqueue", Level.INFO);
        setLogLevel("io.github.colinzhu.taskqueue.TaskRepo", Level.DEBUG);

        pool = TestHelper.getJdbcPool(vertx);
        TestHelper.createTables(pool).onComplete(ar -> testContext.completeNow());
    }

    private static void setLogLevel(String logger, Level level) {
        Logger taskQueueLogger = (Logger) LoggerFactory.getLogger(logger);
        taskQueueLogger.setLevel(level);
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void testNormal(Vertx vertx, VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(2);
        Payment payment = new Payment(System.nanoTime(), "CREATED", "B", System.currentTimeMillis());
        savePayment(payment)
                .compose(p -> pool.withTransaction(sqlConnection -> TaskQueueService.taskQueue().enqueue(sqlConnection, "Q1", "ref1", payment)))
                .onComplete(testContext.succeeding(p -> checkpoint.flag()));

        Function<Task<Payment>, Future<Integer>> taskProcessor = task -> {
            log.info("Processing {}", task.getPayload());
            return pool.withTransaction(sqlConnection -> {
                        return sqlConnection.query("UPDATE PAYMENT SET STATUS = 'PROCESSED' WHERE ID = " + task.getId())
                                .execute()
                                .map(SqlResult::size)
                                .compose(result -> TaskQueueService.taskQueue().finish(sqlConnection, task.getId()));
                    })
                    .compose(result -> pool.withConnection(sqlConnection -> retrievePayment(sqlConnection, task.getId())))
                    .map(result -> {
                        Assertions.assertEquals("PROCESSED", result.getStatus(), "PAYMENT status should be changed");
                        return result;
                    })
                    .compose(result -> pool.withConnection(sqlConnection -> retrieveTask(sqlConnection, task.getId())))
                    .map(t -> {
                        Assertions.assertNull(t, "task should be deleted");
                        checkpoint.flag();
                        return 1;
                    });
        };

        PollConfig<Payment> pollConfig = new PollConfig<>("Q1", 5, Duration.ofMinutes(10), taskProcessor, Payment.class);
        TaskPoller<Payment> poller = new TaskPoller<>(vertx, pool, pollConfig);
        poller.start();
    }

    private Future<Long> savePayment(Payment payment) {
        return pool.preparedQuery("insert into PAYMENT (STATUS, INSTANCE, CREATE_TIME) values (?, ?, ?)")
                .execute(Tuple.of(payment.getStatus(), payment.getInstance(), payment.getCreateTime()))
                .map(rows -> rows.property(JDBCPool.GENERATED_KEYS).getLong(0));
    }

    private Future<Payment> retrievePayment(SqlConnection sqlConnection, Long id) {
        return sqlConnection.query("SELECT * FROM PAYMENT WHERE ID = " + id)
                .execute()
                .map(rows -> rows.iterator().next())
                .map(row -> {
                    if (null == row) {
                        return null;
                    }
                    return new Payment(row.getLong("ID"), row.getString("STATUS"), row.getString("INSTANCE"), row.getLong("CREATE_TIME"));
                });
    }

    private Future<Row> retrieveTask(SqlConnection sqlConnection, Long id) {
        return sqlConnection.query("SELECT * FROM TASKS WHERE ID = " + id)
                .execute()
                .map(rows -> rows.iterator().hasNext() ? rows.iterator().next() : null);
    }

}