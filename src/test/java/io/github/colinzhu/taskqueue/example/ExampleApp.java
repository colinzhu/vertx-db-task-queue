package io.github.colinzhu.taskqueue.example;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.github.colinzhu.taskqueue.H2Database;
import io.github.colinzhu.taskqueue.example.check.PaymentCheckTaskProcessor;
import io.github.colinzhu.taskqueue.example.check.PaymentCheckVerticle;
import io.github.colinzhu.taskqueue.example.release.PaymentReleaseVerticle;
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
        setLogLevel("io.github.colinzhu.taskqueue", Level.INFO);
        setLogLevel("io.github.colinzhu.taskqueue.TaskPoller", Level.DEBUG);
        setLogLevel("io.github.colinzhu.taskqueue.TaskEntityRepo", Level.DEBUG);
        setLogLevel(PaymentCheckTaskProcessor.class.getName(), Level.INFO);

        Vertx vertx = Vertx.vertx();
        H2Database.createH2Server();

        JDBCPool pool = H2Database.getJdbcPool(vertx, false);
        H2Database.createTables(pool)
                .onSuccess(tablesCreated -> {
                    vertx.deployVerticle(PaymentCheckVerticle.class, new DeploymentOptions().setInstances(2))
                            .compose(any -> vertx.deployVerticle(PaymentReleaseVerticle.class, new DeploymentOptions().setInstances(2)))
                            .compose(any -> vertx.deployVerticle(WebVerticle::new, new DeploymentOptions().setInstances(2)))
                            .onFailure(err -> log.error("error", err));
                })
                .onFailure(err -> log.error("Unable to create tables", err));

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
