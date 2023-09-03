package io.github.colinzhu.taskqueue;

import lombok.*;

import java.io.Serializable;
import java.time.ZonedDateTime;

@AllArgsConstructor
@ToString
@Data
class TaskEntity implements Serializable {
    private long id;
    private String referenceNumber;
    private String queueName;
    private String status;
    private long attempt;
    private ZonedDateTime createTime;
    private ZonedDateTime nextProcessTime;
    private ZonedDateTime lastUpdateTime;
    private String payload;
}
