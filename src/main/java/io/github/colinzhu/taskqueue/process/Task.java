package io.github.colinzhu.taskqueue.process;

import lombok.Value;

import java.io.Serializable;

@Value
public class Task<T> implements Serializable {
    long id;
    String queueName;
    String refNumber;
    long attempt;
    T payload;
}
