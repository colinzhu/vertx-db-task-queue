package io.github.colinzhu.taskqueue.polling;

import lombok.Value;

import java.io.Serializable;
@Value
public class Task<T> implements Serializable {
    long id;
    long attempt;
    T payload;
}
