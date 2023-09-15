package io.github.colinzhu.taskqueue.example;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.OffsetDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Payment implements Serializable {
    private Long id;
    private String status;
    private OffsetDateTime createTime;

    public Payment(String status, OffsetDateTime createTime) {
        this.status = status;
        this.createTime = createTime;
    }
}