package io.github.colinzhu.taskqueue.example;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Verticle;
import io.vertx.core.Vertx;
import io.vertx.jdbcclient.JDBCPool;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.LoggerFactory;

import static org.slf4j.Logger.ROOT_LOGGER_NAME;

@Slf4j
public class ExampleApp {
    @SneakyThrows
    public static void main(String[] args) {
        setLogLevel(ROOT_LOGGER_NAME, Level.INFO);
        setLogLevel("com.mchange.v2.resourcepool.BasicResourcePool", Level.INFO);
        setLogLevel("io.github.colinzhu.taskqueue", Level.INFO);
        setLogLevel("io.github.colinzhu.taskqueue.TaskRepo", Level.WARN);
        setLogLevel(PaymentCheckTaskProcessor.class.getName(), Level.INFO);

        Vertx vertx = Vertx.vertx();
        H2Database.main();
        JDBCPool pool = H2Database.getJdbcPool(vertx);
        H2Database.createTables(pool)
                .onSuccess(tablesCreated -> {
                    Verticle createVerticle = new PaymentCreateVerticle(pool);
                    vertx.deployVerticle(PaymentCheckVerticle.class, new DeploymentOptions().setInstances(2))
                            .compose(any -> vertx.deployVerticle(createVerticle))
                            .onFailure(err -> log.error("error", err));
                })
                .onFailure(err -> log.error("Unable to create tables", err));
    }

    private static void setLogLevel(String logger, Level level) {
        Logger taskQueueLogger = (Logger) LoggerFactory.getLogger(logger);
        taskQueueLogger.setLevel(level);
    }
}
