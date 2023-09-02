package io.github.colinzhu.taskqueue.example;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Payment implements Serializable {
    private Long id;
    private String status;
    private Long createTime;

    public Payment(String status, Long createTime) {
        this.status = status;
        this.createTime = createTime;
    }
}