module io.github.colinzhu.taskqueue {
    requires com.fasterxml.jackson.datatype.jsr310;
    requires com.fasterxml.jackson.databind;
    requires io.vertx.core;
    requires io.vertx.client.sql;
    requires io.vertx.client.jdbc;
    requires io.vertx.metrics.micrometer;
    requires io.vertx.client.sql.templates;
    requires io.vertx.web;
    requires micrometer.core;
    requires static lombok;
    requires org.slf4j;
    requires ch.qos.logback.core;
    requires ch.qos.logback.classic;
    requires io.vertx.web.client;

    exports io.github.colinzhu.taskqueue.bridge;
    exports io.github.colinzhu.taskqueue.dispatch;
    exports io.github.colinzhu.taskqueue.process;
    exports io.github.colinzhu.taskqueue.enqueue;
    exports io.github.colinzhu.taskqueue.support;

    opens io.github.colinzhu.taskqueue.internal to com.fasterxml.jackson.databind, lombok;
    opens io.github.colinzhu.taskqueue.bridge to lombok;
    opens io.github.colinzhu.taskqueue.dispatch to lombok;
    opens io.github.colinzhu.taskqueue.support to lombok;
    opens io.github.colinzhu.taskqueue.enqueue to lombok;
    opens io.github.colinzhu.taskqueue.process to lombok;

}

