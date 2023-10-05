package io.github.colinzhu.taskqueue;

import lombok.*;

import java.io.Serializable;
import java.time.OffsetDateTime;

@Value
@Builder
class TaskEntity implements Serializable {
    long id;
    String referenceNumber;
    String queueName;
    TaskStatus status;
    long attempt;
    OffsetDateTime createTime;
    OffsetDateTime nextProcessTime;
    OffsetDateTime lastUpdateTime;
    @ToString. Exclude
    String payload;
}
