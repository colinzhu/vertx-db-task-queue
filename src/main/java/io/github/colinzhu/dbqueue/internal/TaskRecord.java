package io.github.colinzhu.dbqueue.internal;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.ZonedDateTime;

@Data
@AllArgsConstructor
public class TaskRecord {
    private long id;
    private String queueName;
//    private long attemptCount;
    private String status;
    private String payload;
    private String referenceNumber;
    private ZonedDateTime createTime;
    private ZonedDateTime nextProcessTime;
    private ZonedDateTime lastUpdateTime;
}
