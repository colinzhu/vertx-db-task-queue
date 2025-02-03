# Vert.x DB Task Queue 设计文档

## 1. 系统概述

Vert.x DB Task Queue 是一个基于数据库实现的轻量级任务队列系统，它提供了一种不依赖专门消息队列中间件的异步任务处理方案。系统利用数据库作为持久化存储，结合 Vert.x 的异步处理能力，实现了高可用、可靠的任务处理机制。

### 1.1 设计目标

- 简单性：提供简洁的 API，降低使用门槛
- 可靠性：确保任务不丢失，支持故障恢复
- 可扩展性：支持多实例部署，水平扩展
- 可监控性：提供完整的任务状态监控和管理功能
- 轻量级：最小化外部依赖，仅需数据库支持

## 2. 核心架构

### 2.1 系统组件

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Application   │     │   Task Queue    │     │    Database     │
│                 │     │    Service      │     │                 │
│ ┌─────────────┐ │     │ ┌─────────────┐ │     │ ┌─────────────┐ │
│ │Task Producer│─┼────>│ │Task Enqueue │ │────>│ │  Tasks      │ │
│ └─────────────┘ │     │ └─────────────┘ │     │ │  Table      │ │
│                 │     │                 │     │ │             │ │
│ ┌─────────────┐ │     │ ┌─────────────┐ │     │ │             │ │
│ │Task Consumer│<┼─────│ │Task Poller  │ │<────│ │             │ │
│ └─────────────┘ │     │ └─────────────┘ │     │ └─────────────┘ │
└─────────────────┘     └─────────────────┘     └─────────────────┘
```

### 2.2 核心组件说明

1. **TaskQueueService**
   - 提供任务队列的核心操作接口
   - 负责任务的入队、完成、重入队等操作
   - 确保事务一致性

2. **TaskPollerVerticle**
   - 负责任务轮询的核心组件
   - 管理轮询生命周期
   - 支持动态启停

3. **TaskProcessorVerticle**
   - 负责具体任务的处理
   - 支持自定义处理逻辑
   - 处理结果反馈

4. **TaskQueueSupportService**
   - 提供任务监控和管理功能
   - 支持任务状态查询和管理
   - 提供 Web UI 界面

## 3. 关键设计决策

### 3.1 任务状态流转

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

### 3.2 并发处理机制

系统采用两种方案处理多实例并发问题：

#### 方案1：Select for Update
```sql
SELECT * FROM TASKS 
WHERE STATUS = 'CREATED' 
AND QUEUE_NAME = ? 
FOR UPDATE SKIP LOCKED
```

#### 方案2：乐观更新
```sql
UPDATE TASKS SET 
  STATUS = 'PROCESSING', 
  POLLER_INSTANCE = ?, 
  NEXT_PROCESS_TIME = ? 
WHERE ID IN (SELECT ID FROM TASKS 
            WHERE STATUS = 'CREATED' 
            AND QUEUE_NAME = ?)
```

### 3.3 数据模型

**Tasks 表结构**

| 字段名            | 类型      | 说明                     |
|------------------|-----------|------------------------|
| ID               | NUMBER    | 主键                    |
| QUEUE_NAME       | VARCHAR2  | 队列名称                 |
| REFERENCE_NUMBER | VARCHAR2  | 任务引用号（业务标识）      |
| STATUS           | VARCHAR2  | 任务状态                 |
| ATTEMPT          | NUMBER    | 尝试次数                 |
| PAYLOAD          | CLOB      | 任务数据                 |
| PROCESS_RESULT   | VARCHAR2  | 处理结果                 |
| CREATE_TIME      | TIMESTAMP | 创建时间                 |
| POLLER_INSTANCE  | VARCHAR2  | 处理实例标识             |
| NEXT_PROCESS_TIME| TIMESTAMP | 下次处理时间             |

## 4. 关键实现细节

### 4.1 任务入队

```java
Future<Long> enqueue(SqlConnection conn, String queueName, String refNumber, T payload) {
    // 1. 序列化任务数据
    // 2. 在事务中保存任务
    // 3. 通知轮询器立即检查新任务
}
```

### 4.2 任务轮询

- 使用定时器定期检查任务
- 支持动态调整轮询间隔
- 实现退避策略处理空闲情况

### 4.3 错误处理

1. 任务重试机制
   - 支持配置重试次数
   - 支持延迟重试
   - 支持自定义重试策略

2. 毒药任务处理
   - 标记无法处理的任务
   - 支持人工干预
   - 提供清理机制

## 5. 扩展性设计

### 5.1 支持的扩展点

1. 任务处理器
   - 自定义业务逻辑
   - 自定义错误处理
   - 自定义重试策略

2. 存储层
   - 支持不同数据库
   - 支持自定义表结构
   - 预留 Redis 支持

### 5.2 监控与管理

1. 指标收集
   - 任务处理时间
   - 成功/失败率
   - 队列深度

2. 管理功能
   - 任务查询
   - 状态修改
   - 手动触发

## 6. 最佳实践

### 6.1 事务处理

```java
pool.withTransaction(conn -> 
    // 1. 执行业务逻辑
    businessLogic(conn)
        // 2. 入队任务
        .compose(result -> 
            taskQueueService.enqueue(conn, "queue", "ref", payload))
);
```

### 6.2 任务处理器实现

```java
class CustomTaskProcessor implements Function<Task<T>, Future<?>> {
    @Override
    public Future<?> apply(Task<T> task) {
        return Future.future(promise -> {
            // 1. 解析任务数据
            // 2. 执行业务逻辑
            // 3. 更新任务状态
        });
    }
}
```

## 7. 未来优化方向

1. 任务优先级支持
2. 分布式锁优化
3. 批量任务处理
4. 任务依赖关系
5. 更多存储引擎支持
6. 性能优化
   - 连接池调优
   - SQL 优化
   - 缓存策略 

## 8. 模块划分

系统被划分为以下几个主要模块：

### 8.1 模块概览

1. **核心模块（core/internal）**
   - 包路径：`io.github.colinzhu.taskqueue` 和 `io.github.colinzhu.taskqueue.internal`
   - 主要组件：
     - `TaskQueueService` - 任务队列服务接口
     - `TaskQueueServiceImpl` - 服务实现类
     - `TaskEntity` - 内部任务实体
     - `TaskStatus` - 任务状态枚举（包级私有）
     - `TaskRepo` - 数据访问层
   - 主要职责：
     - 提供任务队列的核心操作
     - 管理任务状态和生命周期
     - 处理数据持久化
     - 提供基础工具方法

2. **轮询模块（polling）**
   - 包路径：`io.github.colinzhu.taskqueue.polling`
   - 主要组件：
     - `TaskPoller` - 任务轮询器
     - `TaskPollerVerticle` - 轮询Verticle
     - `TaskPollerConfig` - 轮询配置
   - 主要职责：
     - 任务发现和获取
     - 任务处理和执行
     - 轮询生命周期管理
     - 并发控制

3. **支持模块（support）**
   - 包路径：`io.github.colinzhu.taskqueue.support`
   - 主要组件：
     - `TaskQueueSupportService` - 支持服务接口
     - `TaskQueueSupportHandler` - Web处理器
   - 主要职责：
     - 提供管理界面
     - 任务监控和统计
     - 问题任务处理
     - 系统维护功能

### 8.2 模块架构图

```
┌─────────────────────────────────────────────────────┐
│                    应用层 Application                │
└─────────────────────────────────────────────────────┘
           ↑               ↑                ↑
           │               │                │
┌─────────────┐    ┌──────────────┐  ┌──────────────┐
│  核心模块    │    │   轮询模块     │  │  支持模块     │
│core/internal│    │   polling    │  │  support     │
└─────────────┘    └──────────────┘  └──────────────┘
```

### 8.3 模块间依赖关系

1. **核心模块（core/internal）**
   - 不依赖其他模块
   - 通过 module-info.java 控制包的可见性
   - 内部实现放在 internal 包中

2. **轮询模块（polling）**
   - 依赖核心模块
   - 通过 module-info.java 获得对 core 包的访问权限
   - 实现任务处理逻辑

3. **支持模块（support）**
   - 依赖核心模块
   - 通过 module-info.java 获得对 core 包的访问权限
   - 实现Web界面

### 8.4 访问控制

1. **包级私有类**
   - `TaskStatus` - 仅在 internal 包内可见
   - `TaskEntity` - 仅在 internal 包内可见
   - `TaskQueueServiceImpl` - 仅在根包内可见
   - `TaskRepo` - 仅在 internal 包内可见

2. **公开接口**
   - `TaskQueueService` - 主要服务接口
   - `TaskQueueSupportService` - 支持服务接口

3. **模块化控制**
   - 通过 module-info.java 严格控制包的可见性
   - 只向外暴露必要的 API
   - 内部实现完全隐藏

## 9. 最佳实践

### 9.1 事务处理

```java
pool.withTransaction(conn -> 
    // 1. 执行业务逻辑
    businessLogic(conn)
        // 2. 入队任务
        .compose(result -> 
            taskQueueService.enqueue(conn, "queue", "ref", payload))
);
```

### 9.2 任务处理器实现

```java
class CustomTaskProcessor implements Function<Task<T>, Future<?>> {
    @Override
    public Future<?> apply(Task<T> task) {
        return Future.future(promise -> {
            // 1. 解析任务数据
            // 2. 执行业务逻辑
            // 3. 更新任务状态
        });
    }
}
``` 