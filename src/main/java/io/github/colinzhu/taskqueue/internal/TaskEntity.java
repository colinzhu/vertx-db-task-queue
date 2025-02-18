package io.github.colinzhu.taskqueue.internal;

import lombok.Builder;
import lombok.ToString;
import lombok.Value;

import java.io.Serializable;
import java.time.OffsetDateTime;

@Value
@Builder
public class TaskEntity implements Serializable {
    long id;
    String referenceNumber;
    String queueName;
    TaskStatus status;
    long attempt;
    String pollerInstance;
    OffsetDateTime createTime;
    OffsetDateTime nextProcessTime;
    OffsetDateTime lastUpdateTime;
    @ToString.Exclude
    String payload;
    @ToString.Exclude
    String processResult;
}
