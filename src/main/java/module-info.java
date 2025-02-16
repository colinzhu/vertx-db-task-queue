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

    exports io.github.colinzhu.taskqueue.polling;
    exports io.github.colinzhu.taskqueue.processing;
    exports io.github.colinzhu.taskqueue.enqueue;
    exports io.github.colinzhu.taskqueue.support;

    opens io.github.colinzhu.taskqueue.internal to com.fasterxml.jackson.databind, lombok;
    opens io.github.colinzhu.taskqueue.polling to lombok;
    opens io.github.colinzhu.taskqueue.support to lombok;
    opens io.github.colinzhu.taskqueue.enqueue to lombok;
    opens io.github.colinzhu.taskqueue.processing to lombok;

}

