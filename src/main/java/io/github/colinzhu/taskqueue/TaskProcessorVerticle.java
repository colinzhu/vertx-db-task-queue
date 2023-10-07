package io.github.colinzhu.taskqueue;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.Message;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.function.Function;

@Slf4j
@RequiredArgsConstructor
public class TaskProcessorVerticle<T> extends AbstractVerticle {
    private final String queueName;
    private final Function<Task<T>, Future<?>> taskProcessor;
    private String id;

    @Override
    public void start() {
        id = "taskHandlerVerticle-" + queueName + "-" + Integer.toHexString(this.hashCode());
        vertx.eventBus().consumer(queueName, this::handle);
        log.info("{} created", id);
    }

    private void handle(Message<Task<T>> message) {
        log.info("{} task received, taskId={}", id, message.body().getId());
        Future.succeededFuture()
                .compose(any -> taskProcessor.apply(message.body()))
                .onSuccess(message::reply)
                .onFailure(err -> message.fail(1, getStackTrace(err)));
    }

    private String getStackTrace(Throwable t) {
        StringWriter sw = new StringWriter();
        t.printStackTrace(new PrintWriter(sw));
        return sw.toString();
    }
}
