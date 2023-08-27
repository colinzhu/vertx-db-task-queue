package io.github.colinzhu.taskqueue;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class Task implements Serializable {
    private long id;
    private String payload;
}
