# Vert.x DB Task Queue

A lightweight, database-backed task queue implementation based on Vert.x. It provides a reliable asynchronous task processing solution without the need for dedicated message queue middleware.

## Overview

Vert.x DB Task Queue uses a database as persistent storage and leverages Vert.x's asynchronous capabilities to achieve high availability and reliable task processing. It's designed to be simple, reliable, and easy to integrate into existing systems.

## Features

- **Simple Integration**
  - Minimal dependencies (only requires a database)
  - Clean and intuitive API
  - Easy to set up and configure

- **Reliable Processing**
  - Persistent task storage
  - Transaction support
  - Automatic retry mechanism
  - Poison task handling

- **Scalable Architecture**
  - Multi-instance support
  - Concurrent processing
  - Horizontal scaling capability

- **Operational Excellence**
  - Web-based management UI
  - Task monitoring and statistics
  - Runtime control (pause/resume)
  - Task lifecycle management

- **Flexible Integration**
  - HTTP bridge support
  - Custom task processor
  - Cross-system task transfer
  - JSON-based payload

## Key Design

### 1. How does multiple pollers fetch same tasks from same queue without conflict? There are 2 approaches:

- Approach 1: select tasks for update skip locked -> update conditions so that the tasks will not be selected by another instance
- Approach 2: all instances try to update tasks, if they try to update the same tasks, one will succeed, the others will fail with update count 0 ->  select the updated tasks
  - For example, initial state like this:

    | ID | Queue | Status     | Poller Instance | Next Process Time | Remark                                                                                                  |
    |----|-------|------------|-----------------|-------------------|---------------------------------------------------------------------------------------------------------|
    | 1  | A     | CREATED    |                 | now               | will be checked out now                                                                                 |
    | 2  | A     | CREATED    |                 | now - 3sec        | will be checked out now                                                                                 |
    | 3  | A     | PROCESSING | 222             | now - 3sec        | will be checked out now, though it's marked with poller '222', but the next process time already passed |
    | 4  | A     | PROCESSING | 111             | 15 mins later     | being processed by '111'                                                                                |
    | 5  | B     | CREATED    |                 |                   |                                                                                                         |

  - now update with below SQL, it will be:
    ``` oracle-sql
    UPDATE TASKS SET 
      ATTEMPT = ATTEMPT + 1, STATUS = 'PROCESSING', POLLER_INSTANCE = #{pollerInstance}, NEXT_PROCESS_TIME = #{newNextProcessTime}, LAST_UPDATE_TIME = #{now} 
    WHERE 
      (ID, LAST_UPDATE_TIME) IN (
        (SELECT ID, LAST_UPDATE_TIME FROM TASKS WHERE STATUS IN ('CREATED','PROCESSING')
        AND QUEUE_NAME = #{queueName} 
        AND NEXT_PROCESS_TIME <= #{now} ORDER BY NEXT_PROCESS_TIME FETCH FIRST #{batchSize} ROWS ONLY)
      )
      AND NEXT_PROCESS_TIME <= #{now}  
    ```
    | ID | Queue | Status     | Poller Instance | Next Process Time | Remark               |
    |----|-------|------------|-----------------|-------------------|----------------------|
    | 1  | A     | PROCESSING | 333             | 15 mins later     | checked out by '333' |
    | 2  | A     | PROCESSING | 333             | 15 mins later     | checked out by '333' |
    | 3  | A     | PROCESSING | 333             | 15 mins later     | checked out by '333' |
    | 4  | A     | PROCESSING | 111             | 15 mins later     | checked out by '111' |
    | 5  | B     | CREATED    |                 |                   |                      |
  - then select with SQL
    ```oracle-sql
    SELECT * FROM TASKS 
    WHERE 
      STATUS = 'PROCESSING' 
      AND QUEUE_NAME = #{queueName} 
      AND POLLER_INSTANCE = #{pollerInstance} 
      AND NEXT_PROCESS_TIME > #{now}
  ```

### 2. Task States

```
┌─────────┐     ┌────────────┐     ┌───────────┐
│ CREATED │────>│ PROCESSING │────>│ COMPLETED │
└─────────┘     └────────────┘     └───────────┘
     │                │                  
     │                │                  
     │                v                  
     │           ┌────────┐        ┌─────────┐
     └──────────>│ ERROR  │───────>│ POISON  │
                 └────────┘        └─────────┘
```

## Getting Started

### 1. Add Dependency

```xml
<dependency>
    <groupId>io.github.colinzhu</groupId>
    <artifactId>vertx-db-task-queue</artifactId>
    <version>${version}</version>
</dependency>
```

### 2. Basic Usage
Refer to `ExampleApp`

### 3. HTTP Bridge
Refer to `ExampleApp`

## Monitoring and Support

The system provides a web-based management UI for:

- Task status monitoring
- Queue statistics
- Manual task control
- Error handling
- System configuration

## Installation
- Create table
```sql
CREATE TABLE TASKS (
  ID NUMBER GENERATED BY DEFAULT AS IDENTITY,
  QUEUE_NAME VARCHAR2(200) NOT NULL,
  REFERENCE_NUMBER VARCHAR2(200),
  STATUS VARCHAR2(30),
  ATTEMPT NUMBER DEFAULT 0,
  PAYLOAD CLOB,
  PROCESS_RESULT VARCHAR2(4000),
  CREATE_TIME TIMESTAMP with TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  POLLER_INSTANCE VARCHAR2(200),
  NEXT_PROCESS_TIME   TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  LAST_UPDATE_TIME   TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT UC_TASK UNIQUE (REFERENCE_NUMBER,QUEUE_NAME)
);
CREATE INDEX IDX_QNM_ST_NXT_PROC_TM ON TASKS(QUEUE_NAME, STATUS, NEXT_PROCESS_TIME);
```

## TODO
- [x] Integrate with event bus - done
- [x] Payload casting - done
- [x] Task processing error handling - done
- [x] reenqueue - done
- [x] junit - done
- [x] expose ATTEMPT in Task
- [x] test: when 2 instances, 1 is down but with checked-out tasks in progress, the 2nd instance will continue to process
- [x] support API: searchByQueueNameAndStatus
- [x] support API: reprocessErrorTasks
- [x] support API: countGroupByQueueNameAndStatus
- [x] support API: rename reprocess to reenqueue
- [x] support API: ERROR -> POISON
- [x] support API: POISON -> ERROR
- [x] support API: housekeep POISON (only task older than x weeks)
- [x] handle concurrent update task case
- [x] remove event-bus impl, set a big batch size in poller can have similar performance
- [x] add support of system property "TaskQueueService.isDeleteWhenFinish"
- [x] update TaskPoller to rerun without delay when has tasks
- [x] when enqueue a task, notify poller by sending an event to event bus, if it's waiting for timer, it can cancel the timer and process immediately
- [x] Update TaskQueueService to non-singleton, because vertx is now parameter to create TaskQueueService
- [x] rename "finish" to "complete" // 2023-10-04
- [x] 2023-10-05 consider to split "complete" into "complete" and "completeDelete"
- [x] 2023-10-05 remove system property "TaskQueueService.isDeleteWhenFinish", because now support "complete" and "completeDelete"
- [x] 2023-10-05 change updateStatus to updateStatusFrom
- [x] 2023-10-05 create TaskStatus enum
- [x] 2023-10-05 update junit to make it more clear and reliable
- [x] 2023-10-05 consider to move TaskSupportVerticle from test to main, create TaskQueueSupportHandler
- [x] 2023-10-05 get bootstrap and tabulator from webjar by maven build
- [x] 2023-10-05 rename PollConfig to TaskPollerConfig
- [x] 2023-10-06 make TaskPollerConfig mutable, so that there is a flexibility to change the config during runtime // 2023-10-06
- [x] 2023-10-06 rename TaskPollerConfig.nextProcessDelay to deadline
- [x] 2023-10-06 rename TaskEntityRepo to TaskQueueRepo
- [x] 2023-10-07 create common TaskPollerVerticle and TaskProcessorVerticle
- [x] 2023-10-07 TaskPollerVerticle invokes TaskProcessorVerticle through event bus (request and response)
- [x] 2023-10-11 update poller config to add timeout - event bus sendTimeout, rename deadline to nextProcessDelay
- [x] 2023-10-11 update example to support different databases
- [x] 2023-10-11 change TaskPoller to public as part of the core API
- [x] 2023-10-11 enhance poller shutdown logic
- [x] test with 4 instances (JVM) to poll tasks from one DB instance, 1. no duplicate processing 2. no error
- [x] test request timeout which should mark as ERROR
- [x] 2023-10-21 support storing process result for failure case for problem investigation, max 4000 bytes
- [x] 2023-10-21 support storing process result for complete case
- [x] 2023-10-21 support storing process result for reenqueue case
- [x] 2023-10-21 support unique task - add unique key for queueName + referenceNumber
- [x] 2023-10-21 add example to implement retry by using requeue feature
- [x] 2023-10-22 add another checkout implementation, doesn't lock records to prevent records being locked by zombie connections
- [x] 2023-10-22 add retry for update task status, in case DB exception at that time
- [x] 2023-10-23 for checkout2 implementation, split 'update' and 'select' into to connections instead of one transaction
- [x] 2023-10-24 JDBCPool should only be created by verticle itself, if it's created by another component, it can be shutdown first by another component
- [x] 2023-10-24 study and draw the new checkout impl
- [x] 2023-10-25 support parameter to control poller to start or not when verticle is deployed
- [x] 2023-10-25 support start and stop poller via event bus and support API
- [x] 2023-11-09 add micrometer
- [x] 2023-12-10 change support UI layout - swap row and column
- [x] 2023-12-11 enhance DB error retry logic
- [x] 2025-02-03 split logics into modules in different packages
- [x] 2025-02-18 add task bridge feature
- [ ] study the UUID poller instance impact
- [ ] support start and stop poller from support web page
- [ ] integrate with 'workflow engine'
- [ ] consider to change task ID from long to UUID
- [ ] house keep - regularly move records to history table
- [ ] house keep - regularly delete records in history table
- [ ] support other store e.g. redis

