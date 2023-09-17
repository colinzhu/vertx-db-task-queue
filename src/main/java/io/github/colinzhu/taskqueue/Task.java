package io.github.colinzhu.taskqueue;

import lombok.Value;

import java.io.Serializable;
@Value
public class Task<T> implements Serializable {
    long id;
    long attempt;
    T payload;
}
