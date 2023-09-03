package io.github.colinzhu.taskqueue;

import lombok.Data;

import java.io.Serializable;
@Data
public class Task<T> implements Serializable {
    private final long id;
    private final T payload;
}
