package io.github.colinzhu.taskqueue;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.github.colinzhu.taskqueue.dispatch.TaskDispatchConfig;
import io.github.colinzhu.taskqueue.dispatch.TaskDispatchVerticle;
import io.github.colinzhu.taskqueue.example.Payment;
import io.github.colinzhu.taskqueue.process.TaskProcessVerticle;
import io.github.colinzhu.taskqueue.process.TaskProcessor;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.*;
import io.vertx.sqlclient.templates.SqlTemplate;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.function.Supplier;

public class TaskQueueTestUtils {
    static Future<Payment> savePayment(SqlConnection sqlConnection, Payment payment) {
        return sqlConnection.preparedQuery("insert into PAYMENT (STATUS, CREATE_TIME) values (?, ?)")
                .execute(Tuple.of(payment.getStatus(), payment.getCreateTime()))
                .map(rows -> {
                    Long id = rows.property(JDBCPool.GENERATED_KEYS).getLong(0);
                    payment.setId(id);
                    return payment;
                });
    }

    static Future<Integer> updatePayment(SqlConnection sqlConnection, Payment payment) {
        return sqlConnection.query("UPDATE PAYMENT SET STATUS = 'PROCESSED' WHERE ID = " + payment.getId())
                .execute()
                .map(SqlResult::size);
    }

    static Future<Integer> updatePaymentTo(SqlConnection sqlConnection, Payment payment, String status) {
        return sqlConnection.query("UPDATE PAYMENT SET STATUS = '" + status + "' WHERE ID = " + payment.getId())
                .execute()
                .map(SqlResult::size);
    }

    static Future<Payment> retrievePayment(Long id, JDBCPool pool) {
        return pool.query("SELECT * FROM PAYMENT WHERE ID = " + id)
                .execute()
                .map(rows -> {
                    RowIterator<Row> iterator = rows.iterator();
                    return iterator.hasNext() ? iterator.next() : null;
                })
                .map(row -> {
                    if (null == row) {
                        return null;
                    }
                    return new Payment(row.getLong("ID"), row.getString("STATUS"), row.getOffsetDateTime("CREATE_TIME"));
                });
    }

    static Future<Row> retrieveTask(String refNbr, JDBCPool pool) {
        return SqlTemplate.forQuery(pool, "SELECT * FROM TASKS WHERE REFERENCE_NUMBER = #{refNbr} ")
                .execute(Map.of("refNbr", refNbr))
                .map(rows -> {
                    RowIterator<Row> iterator = rows.iterator();
                    return iterator.hasNext() ? iterator.next() : null;
                });
    }

    static TaskDispatchConfig<Payment> createTaskDispatchConfig(String queueName) {
        return new TaskDispatchConfig<>(queueName, Payment.class)
                .setNoTaskPollInterval(Duration.ofMillis(500));
    }

    static Future<String> deployDispatchAndProcessVerticles(Vertx vertx, TaskDispatchConfig<Payment> config, TaskProcessor<Payment> taskProcessor, Supplier<JDBCPool> poolSupplier) {
        return vertx.deployVerticle(() -> new TaskProcessVerticle<>(config.getQueueName(), () -> taskProcessor), new DeploymentOptions().setInstances(1))
                .compose(any -> vertx.deployVerticle(() -> new TaskDispatchVerticle<>(poolSupplier, config), new DeploymentOptions().setInstances(1)));
    }

    static void setLogLevel(String logger, Level level) {
        Logger taskQueueLogger = (Logger) LoggerFactory.getLogger(logger);
        taskQueueLogger.setLevel(level);
    }

    static String getQueueName(String action) {
        return switch (action) {
            case "COMPLETE" -> "Q1-need-complete";
            case "REENQUEUE" -> "Q2-need-reenquueue";
            case "COMPLETE_DELETE" -> "Q3-need-completeDelete";
            case "ERR_IN_TXN" -> "Q-ERR_IN_TXN";
            case "ERR_BEFORE_TXN" -> "Q-ERR_BEFORE_TXN";
            default -> null;
        };
    }
}
