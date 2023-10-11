package io.github.colinzhu.taskqueue;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.jdbcclient.JDBCPool;

public interface Database {
    String H2_MEM = "H2-memory";
    String ORACLE = "ORACLE";
    String H2 = "H2";

    static Database get(String dbType) {
        return switch (dbType) {
            case H2_MEM -> new H2Database(true);
            case ORACLE -> null;
            default -> new H2Database(false);
        };
    }

    Future<?> createTables(JDBCPool pool);

    void startServer();

    JDBCPool getJdbcPool(Vertx vertx);

}
