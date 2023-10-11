package io.github.colinzhu.taskqueue.example;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.github.colinzhu.taskqueue.*;
import io.github.colinzhu.taskqueue.example.check.PaymentCheckTaskProcessor;
import io.github.colinzhu.taskqueue.example.release.PaymentReleaseTaskProcessor;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.jdbcclient.JDBCPool;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.slf4j.Logger.ROOT_LOGGER_NAME;

@Slf4j
public class ExampleApp {
    @SneakyThrows
    public static void main(String[] args) {
        setLogLevel(ROOT_LOGGER_NAME, Level.INFO);
        setLogLevel("com.mchange.v2.resourcepool.BasicResourcePool", Level.DEBUG);
        setLogLevel("io.github.colinzhu.taskqueue", Level.DEBUG);
        setLogLevel("io.github.colinzhu.taskqueue.TaskPoller", Level.DEBUG);
        setLogLevel("io.github.colinzhu.taskqueue.TaskQueueRepo", Level.DEBUG);

        Vertx vertx = Vertx.vertx();
        Database db = Database.get(Database.H2);
        db.startServer();

        JDBCPool pool = db.getJdbcPool(vertx);
        db.createTables(pool)
                .onSuccess(tablesCreated -> deployVerticles(vertx, pool))
                .onFailure(err -> log.error("Unable to create tables", err));

        appendShutdownHook(vertx);
    }

    private static void deployVerticles(Vertx vertx, JDBCPool pool) {
        TaskPollerConfig<Payment> taskPollerCheckConfig = new TaskPollerConfig<>("payment.check", Payment.class).setBatchSize(50);
        TaskPollerConfig<Payment> taskPollerReleaseConfig = new TaskPollerConfig<>("payment.release", Payment.class).setBatchSize(50);
        PaymentCheckTaskProcessor paymentCheckTaskProcessor = new PaymentCheckTaskProcessor(vertx, pool, TaskQueueService.taskQueue(vertx));
        PaymentReleaseTaskProcessor paymentReleaseTaskProcessor = new PaymentReleaseTaskProcessor(vertx, pool, TaskQueueService.taskQueue(vertx));

        vertx.deployVerticle(() -> new TaskProcessorVerticle<>(taskPollerCheckConfig.getQueueName(), paymentCheckTaskProcessor), new DeploymentOptions().setInstances(1))
                .compose(any -> vertx.deployVerticle(() -> new TaskProcessorVerticle<>(taskPollerReleaseConfig.getQueueName(), paymentReleaseTaskProcessor), new DeploymentOptions().setInstances(1)))
                .compose(any -> vertx.deployVerticle(() -> new TaskPollerVerticle<>(pool, taskPollerCheckConfig), new DeploymentOptions().setInstances(1)))
                .compose(any -> vertx.deployVerticle(() -> new TaskPollerVerticle<>(pool, taskPollerReleaseConfig), new DeploymentOptions().setInstances(1)))
                .compose(any -> vertx.deployVerticle(() -> new WebVerticle(pool), new DeploymentOptions().setInstances(2)))
                .onFailure(err -> log.error("error", err));
    }

    private static void appendShutdownHook(Vertx vertx) {
        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> {
                    final AtomicBoolean stopCompleted = new AtomicBoolean(false);
                    vertx.close(ar -> stopCompleted.set(true));

                    int maxSeconds = 10;
                    int seconds = 0;
                    while (!stopCompleted.get()) {
                        seconds++;
                        try {
                            Thread.sleep(Duration.ofSeconds(1).toMillis());
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        if (seconds >= maxSeconds) {
                            break;
                        }
                    }
                    System.out.println("Successfully stopped vertx");
                })
        );
    }

    private static void setLogLevel(String logger, Level level) {
        Logger taskQueueLogger = (Logger) LoggerFactory.getLogger(logger);
        taskQueueLogger.setLevel(level);
    }
}
