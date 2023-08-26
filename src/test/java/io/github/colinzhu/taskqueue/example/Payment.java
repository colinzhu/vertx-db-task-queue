package io.github.colinzhu.taskqueue.example;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class Payment implements Serializable {
    private Long id;
    private String status;
    private String instance;
    private Long createTime;
}