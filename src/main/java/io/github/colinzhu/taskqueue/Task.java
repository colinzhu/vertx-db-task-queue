package io.github.colinzhu.taskqueue;

import lombok.*;

import java.io.Serializable;
import java.time.ZonedDateTime;

@AllArgsConstructor
@ToString
public class Task<T> implements Serializable {
    @Getter @Setter(AccessLevel.PACKAGE)
    private long id;

    @Getter(AccessLevel.PACKAGE) @Setter(AccessLevel.PACKAGE)
    private String referenceNumber;

    @Getter(AccessLevel.PACKAGE) @Setter(AccessLevel.PACKAGE)
    private String queueName;

    @Getter(AccessLevel.PACKAGE) @Setter(AccessLevel.PACKAGE)
    private String status;

    @Getter(AccessLevel.PACKAGE) @Setter(AccessLevel.PACKAGE)
    private long attempt;

    @Getter(AccessLevel.PACKAGE) @Setter(AccessLevel.PACKAGE)
    private ZonedDateTime createTime;

    @Getter(AccessLevel.PACKAGE) @Setter(AccessLevel.PACKAGE)
    private ZonedDateTime nextProcessTime;

    @Getter(AccessLevel.PACKAGE) @Setter(AccessLevel.PACKAGE)
    private ZonedDateTime lastUpdateTime;

    @Getter @Setter(AccessLevel.PACKAGE)
    private T payload;
}
