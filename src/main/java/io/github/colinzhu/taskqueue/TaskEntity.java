package io.github.colinzhu.taskqueue;

import lombok.*;

import java.io.Serializable;
import java.time.ZonedDateTime;

@Value
@Builder
class TaskEntity implements Serializable {
    long id;
    String referenceNumber;
    String queueName;
    String status;
    long attempt;
    ZonedDateTime createTime;
    ZonedDateTime nextProcessTime;
    ZonedDateTime lastUpdateTime;
    String payload;
}
