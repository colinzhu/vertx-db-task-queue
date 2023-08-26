package io.github.colinzhu.dbqueue.old;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.time.Duration;

@RequiredArgsConstructor
public class TaskProcessResult {
    @Getter
    private final Type type;
    @Getter
    private final Duration executionDelay;

    private static final TaskProcessResult SUCCESS = new TaskProcessResult(Type.SUCCESS, null);
    private static final TaskProcessResult FAILURE = new TaskProcessResult(Type.FAILURE, null);
    private static final TaskProcessResult REEXECUTE_NO_DELAY = new TaskProcessResult(Type.REEXECUTE, null);
    public enum Type {
        SUCCESS,
        FAILURE,
        REEXECUTE
    }

    public static TaskProcessResult success() {
        return SUCCESS;
    }

    public static TaskProcessResult failure() {
        return FAILURE;
    }

    public static TaskProcessResult reexecute() {
        return REEXECUTE_NO_DELAY;
    }

    public static TaskProcessResult reexecute(Duration delay) {
        return new TaskProcessResult(Type.REEXECUTE, delay);
    }
}
