package io.github.colinzhu.taskqueue.example;

import io.github.colinzhu.taskqueue.H2Database;
import io.github.colinzhu.taskqueue.TaskQueueService;
import io.github.colinzhu.taskqueue.TaskQueueSupportService;
import io.github.colinzhu.taskqueue.TaskQueueSupportHandler;
import io.github.colinzhu.taskqueue.example.create.PaymentCreateHandler;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class WebVerticle extends AbstractVerticle {

    @Override
    public void start() {
        startHttpServer();
    }

    private void startHttpServer() {
        HttpServer server = vertx.createHttpServer();
        Router router = Router.router(vertx);
        router.route("/taskqueue/*").subRouter(new TaskQueueSupportHandler(vertx, H2Database.getJdbcPool(vertx, false), TaskQueueSupportService.getInstance()).get());
        router.route("/taskqueue/*").subRouter(new PaymentCreateHandler(vertx, H2Database.getJdbcPool(vertx, false), TaskQueueService.taskQueue(vertx)).get());

        String logMsg = """
                WebVerticle started, instance={}
                http://localhost:#{port}/taskqueue/create/1
                http://localhost:#{port}/taskqueue/support/api/reenqueue
                http://localhost:#{port}/taskqueue/support/api/mark-poison
                http://localhost:#{port}/taskqueue/support/api/poison-to-error
                http://localhost:#{port}/taskqueue/support/api/housekeep-poison
                http://localhost:#{port}/taskqueue/support/api/search/payment.check/CREATED?size=5
                http://localhost:#{port}/taskqueue/support/api/count
                http://localhost:#{port}/taskqueue/support/web/
                                """;

        server.requestHandler(router).listen(31111)
                .onSuccess(httpServer -> log.info(logMsg.replace("#{port}", String.valueOf(httpServer.actualPort())), Integer.toHexString(this.hashCode())))
                .onFailure(err -> log.error("failed to start task queue support.", err));
    }

}
