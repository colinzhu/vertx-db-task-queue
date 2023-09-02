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
import io.vertx.sqlclient.*;
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
    private static JDBCPool pool;
    private static TaskQueueService taskQueueService;

    @BeforeAll
    static void init(Vertx vertx, VertxTestContext testContext) {
        setLogLevel(ROOT_LOGGER_NAME, Level.INFO);
        setLogLevel("com.mchange.v2.resourcepool.BasicResourcePool", Level.INFO);
        setLogLevel("io.github.colinzhu.taskqueue", Level.INFO);
        setLogLevel("io.github.colinzhu.taskqueue.TaskRepo", Level.DEBUG);

        pool = TestHelper.getJdbcPool(vertx);
        taskQueueService = TaskQueueService.taskQueue(pool);
        TestHelper.createTables(pool).onComplete(ar -> testContext.completeNow());
    }
//
//    @BeforeEach
//    void setUp(Vertx vertx, VertxTestContext testContext) {
////        TestHelper.createTables(pool).onComplete(ar -> testContext.completeNow());
//    }

    private static void setLogLevel(String logger, Level level) {
        Logger taskQueueLogger = (Logger) LoggerFactory.getLogger(logger);
        taskQueueLogger.setLevel(level);
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    @DisplayName("Normal - within one transaction, business entity object updated, tasks finished (deleted)")
    void testNormal(Vertx vertx, VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(3);
        Payment payment = new Payment("CREATED", System.currentTimeMillis());
        savePayment(payment)
                .compose(p -> pool.withTransaction(sqlConnection -> taskQueueService.enqueue(sqlConnection, "Q1", "ref1", p)))
                .onComplete(testContext.succeeding(p -> checkpoint.flag()));

        Function<Task<Payment>, Future<Integer>> taskProcessor = task -> {
            log.info("Processing {}", task.getPayload());
            Future<Integer> updateFuture = taskQueueService.withTaskQueueTxn(
                    conn -> updatePayment(conn, payment),
                    conn -> taskQueueService.finish(conn, task.getId()));

            // verify payment status
            updateFuture.compose(updateCount -> pool.withConnection(conn -> retrievePayment(conn, payment.getId())))
                    .onComplete(testContext.succeeding(res -> Assertions.assertEquals("PROCESSED", res.getStatus(), "PAYMENT status should be changed")))
                    .onComplete(testContext.succeeding(res -> checkpoint.flag()));

            // verify task
            updateFuture.compose(updateCount -> pool.withConnection(conn -> retrieveTask(conn, task.getId())))
                    .onComplete(testContext.succeeding(res -> Assertions.assertNull(res, "task should be deleted")))
                    .onComplete(testContext.succeeding(res -> checkpoint.flag()));

            return updateFuture;
        };

        PollConfig<Payment> pollConfig = new PollConfig<>("Q1", 5, Duration.ofMinutes(10), taskProcessor, Payment.class);
        TaskPoller<Payment> poller = new TaskPoller<>(vertx, pool, pollConfig);
        poller.start();
    }

    @Test
    @DisplayName("TaskProcessingFailed - within one transaction, business entity object updated, but has other exception, business entity object should roll back, tasks should be updated to ERROR")
    void testTaskProcessingFailed_AfterPersistBusinessObject(Vertx vertx, VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(5);
        Payment payment = new Payment("CREATED", System.currentTimeMillis());
        savePayment(payment)
                .compose(p -> pool.withTransaction(sqlConnection -> taskQueueService.enqueue(sqlConnection, "Q1", "ref1", p)))
                .onComplete(testContext.succeeding(task -> {
                    // after 6 seconds
                    vertx.setTimer(6000, id -> {
                        // verify payment status
                        pool.withConnection(conn -> retrievePayment(conn, payment.getId()))
                                .onComplete(testContext.succeeding(p -> {
                                    Assertions.assertEquals("CREATED", p.getStatus(), "PAYMENT status should be rolled back");
                                    checkpoint.flag();
                                }));

                        // verify task
                        pool.withConnection(conn -> retrieveTask(conn, task.getId()))
                                .onComplete(testContext.succeeding(row -> {
                                    Assertions.assertEquals("ERROR", row.getString("STATUS"), "task should be updated to ERROR");
                                    checkpoint.flag();
                                }));
                    });
                    checkpoint.flag();
                }));

        Function<Task<Payment>, Future<Integer>> taskProcessor = task -> {
            log.info("Processing {}", task.getPayload());
            Future<Integer> updateFuture = taskQueueService.withTaskQueueTxn(
                    conn -> updatePayment(conn, payment)
                            .map(updateCount -> {
                                if (1 == 2) {
                                    return updateCount;
                                }
                                throw new RuntimeException("simulate exception during runtime");
                            }),
                    conn -> taskQueueService.finish(conn, task.getId())
            );

            updateFuture.onComplete(res -> {
                Assertions.assertTrue(res.failed());

                // verify payment status
                pool.withConnection(conn -> retrievePayment(conn, payment.getId()))
                        .onComplete(testContext.succeeding(p -> {
                            Assertions.assertEquals("CREATED", p.getStatus(), "PAYMENT status should be rolled back");
                            checkpoint.flag();
                        }));

                // verify task
                pool.withConnection(conn -> retrieveTask(conn, task.getId()))
                        .onComplete(testContext.succeeding(row -> {
                            Assertions.assertEquals("PROCESSING", row.getString("STATUS"), "task status not yet updated to ERROR");
                            checkpoint.flag();
                        }));
            });

            return updateFuture;
        };

        PollConfig<Payment> pollConfig = new PollConfig<>("Q1", 5, Duration.ofMinutes(10), taskProcessor, Payment.class);
        TaskPoller<Payment> poller = new TaskPoller<>(vertx, pool, pollConfig);
        poller.start();
    }

    @Test
    @DisplayName("TaskProcessingFailed - within one transaction, business entity object updated, but has other exception, business entity object should roll back, tasks should be updated to ERROR")
    void testTaskProcessingFailed2(Vertx vertx, VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(3);
        Payment payment = new Payment("CREATED", System.currentTimeMillis());
        savePayment(payment)
                .compose(p -> pool.withTransaction(sqlConnection -> taskQueueService.enqueue(sqlConnection, "Q1", "ref1", p)))
                .onComplete(testContext.succeeding(task -> {
                    // after 6 seconds
                    vertx.setTimer(6000, id -> {
                        // verify payment status
                        pool.withConnection(conn -> retrievePayment(conn, payment.getId()))
                                .onComplete(testContext.succeeding(p -> {
                                    Assertions.assertEquals("CREATED", p.getStatus(), "PAYMENT status should be rolled back");
                                    checkpoint.flag();
                                }));

                        // verify task
                        pool.withConnection(conn -> retrieveTask(conn, task.getId()))
                                .onComplete(testContext.succeeding(row -> {
                                    Assertions.assertEquals("ERROR", row.getString("STATUS"), "task should be updated to ERROR");
                                    checkpoint.flag();
                                }));
                    });
                    checkpoint.flag();
                }));

        Function<Task<Payment>, Future<Integer>> taskProcessor = task -> {
            log.info("Processing {}", task.getPayload());
            Future<Integer> updateFuture = taskQueueService.withTaskQueueTxn(
                    conn -> updatePayment(conn, payment),
                    conn -> taskQueueService.finish(conn, task.getId()));

            // simulate exception
            String a = null;
            a.length();

            return updateFuture;
        };

        PollConfig<Payment> pollConfig = new PollConfig<>("Q1", 5, Duration.ofMinutes(10), taskProcessor, Payment.class);
        TaskPoller<Payment> poller = new TaskPoller<>(vertx, pool, pollConfig);
        poller.start();
    }

    private Future<Payment> savePayment(Payment payment) {
        return pool.preparedQuery("insert into PAYMENT (STATUS, CREATE_TIME) values (?, ?)")
                .execute(Tuple.of(payment.getStatus(), payment.getCreateTime()))
                .map(rows -> {
                    Long id = rows.property(JDBCPool.GENERATED_KEYS).getLong(0);
                    payment.setId(id);
                    return payment;
                });
    }

    private Future<Integer> updatePayment(SqlConnection sqlConnection, Payment payment) {
        return sqlConnection.query("UPDATE PAYMENT SET STATUS = 'PROCESSED' WHERE ID = " + payment.getId())
                .execute()
                .map(SqlResult::size);
    }

    private Future<Payment> retrievePayment(SqlConnection sqlConnection, Long id) {
        return sqlConnection.query("SELECT * FROM PAYMENT WHERE ID = " + id)
                .execute()
                .map(rows -> {
                    RowIterator<Row> iterator = rows.iterator();
                    return iterator.hasNext() ? iterator.next() : null;
                })
                .map(row -> {
                    if (null == row) {
                        return null;
                    }
                    return new Payment(row.getLong("ID"), row.getString("STATUS"), row.getLong("CREATE_TIME"));
                });
    }

    private Future<Row> retrieveTask(SqlConnection sqlConnection, Long id) {
        return sqlConnection.query("SELECT * FROM TASKS WHERE ID = " + id)
                .execute()
                .map(rows -> {
                    RowIterator<Row> iterator = rows.iterator();
                    return iterator.hasNext() ? iterator.next() : null;
                });
    }

}