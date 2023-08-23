package io.github.colinzhu.dbqueue.api;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Task {
    private long id;
    private String payload;
}
