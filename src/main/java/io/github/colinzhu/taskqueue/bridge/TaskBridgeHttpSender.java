package io.github.colinzhu.taskqueue.bridge;

import io.github.colinzhu.taskqueue.process.Task;
import io.github.colinzhu.taskqueue.process.TaskProcessService;
import io.github.colinzhu.taskqueue.process.TaskProcessor;
import io.vertx.core.Future;
import io.vertx.ext.web.client.WebClient;
import io.vertx.jdbcclient.JDBCPool;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class TaskBridgeHttpSender implements TaskProcessor<String> {

    private final WebClient client;
    private final String url;
    private final JDBCPool pool;
    private final TaskProcessService taskProcessService = TaskProcessService.getInstance();

    @Override
    public Future<Void> process(Task<String> task) {
        log.info("Sending task {} to {}", task, url);
        return client.postAbs(url).sendJson(task)
                .onSuccess(res -> log.info("Successfully sent task {} to {}", task, url))
                .onFailure(throwable -> log.error("Error sending task {} to {}", task, url, throwable))
                .map(res -> {
                    if (res.statusCode() != 200) {
                        throw new RuntimeException("Failed to send task to " + url + ", status code: " + res.statusCode() + ", body: " + res.bodyAsString());
                    }
                    return null;
                })
                .compose(voidFuture -> pool.withTransaction(conn -> taskProcessService.complete(conn, task)));
    }
}
