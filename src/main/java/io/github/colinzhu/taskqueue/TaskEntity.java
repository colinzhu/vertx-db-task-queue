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
    String status;
    long attempt;
    OffsetDateTime createTime;
    OffsetDateTime nextProcessTime;
    OffsetDateTime lastUpdateTime;
    String payload;
}
