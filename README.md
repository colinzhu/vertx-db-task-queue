# Vert.x DB Task Queue

## Overview
A DB based task queue with vert.x

## Usage - API
- `Task` - POJO to be processed by task processor `Function<Task<T>, Future<Integer>>`
- `TaskPoller` - Poller to poll tasks from DB and invoke the task processor
- `TaskConfig` - POJO to store the poller config
- `TaskQueueService` - Service to 
  1. put task into queue
  2. mark the task as finished
  3. re-put the task into queue for next processing
  4. mark the task as error

## Example
Please refer to `TaskQueueTest` in test package

## TODO
- [x] Integrate with event bus - done
- [x] Payload casting - done
- [x] Task processing error handling - done
- [x] reenqueue - done
- [x] junit - done
- [ ] API to handle ERROR task
