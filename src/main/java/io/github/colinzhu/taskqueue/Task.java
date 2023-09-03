package io.github.colinzhu.taskqueue;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;

@AllArgsConstructor
@ToString
@Getter
public class Task<T> implements Serializable {
    private long id;
    private T payload;
}
