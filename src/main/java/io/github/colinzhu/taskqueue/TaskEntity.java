package io.github.colinzhu.taskqueue;

import lombok.*;

import java.io.Serializable;
import java.time.ZonedDateTime;

@Data
class TaskEntity implements Serializable {
    private final long id;
    private final String referenceNumber;
    private final String queueName;
    private final String status;
    private final long attempt;
    private final ZonedDateTime createTime;
    private final ZonedDateTime nextProcessTime;
    private final ZonedDateTime lastUpdateTime;
    private final String payload;
}
