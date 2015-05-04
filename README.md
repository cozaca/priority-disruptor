# priority-disruptor 
## A multi-priority Disruptor
## 使用说明
### 示例
#### 启动disruptor：

        ExecutorService executor = Executors.newSingleThreadExecutor();
        multiDisruptor = new MultiPriorityDisruptor<Event>(new EventFactory(), ringSize, executor, ProducerType.MULTI,
                priorities);
        multiDisruptor.handleEventsWith(new Handler());
        multiDisruptor.start();

#### 向disruptor中发送消息：

    boolean publishMulti(boolean sync, int priority, int idx, String value) {
        long sequence = 0;
        if (sync) {
            sequence = multiDisruptor.next(priority);
        } else {
            try {
                sequence = multiDisruptor.tryNext(priority);
            } catch (InsufficientCapacityException ex) {
                return false;
            }
        }
        try {
            multiDisruptor.get(priority, sequence).set(priority, idx, value);
        } finally {
            multiDisruptor.publish(priority, sequence);
        }
        return true;
    }

## Benchmark Result:
### 1 priority-queue:

    Benchmark/Single Ellapsed: 1190.60ms, QPS: 1184336.41. (producerCount=1, ringSize=1024, queries=10000000). 
    Benchmark/Multi  Ellapsed: 1332.59ms, QPS: 1058138.90. (priorities=[10], ringSize=1024x1, queries=10000000).
    Benchmark/Multi  Ellapsed: 1337.38ms, QPS: 1054348.12. (priorities=[200], ringSize=1024x1, queries=10000000).
    Benchmark/Multi  Ellapsed: 1279.93ms, QPS: 1101672.86. (priorities=[1000], ringSize=1024x1, queries=10000000).

### 2 priority-queue:

    Benchmark/Single Ellapsed: 1658.75ms, QPS: 850078.41. (producerCount=2, ringSize=1024, queries=10000000). 
    Benchmark/Multi  Ellapsed: 3545.48ms, QPS: 397708.19. (priorities=[2, 8], ringSize=512x2, queries=10000000).
    Benchmark/Multi  Ellapsed: 2058.11ms, QPS: 685127.08. (priorities=[20, 80], ringSize=512x2, queries=10000000).
    Benchmark/Multi  Ellapsed: 1510.80ms, QPS: 933326.42. (priorities=[200, 800], ringSize=512x2, queries=10000000).

### 4 priority-queue:

    Benchmark/Single Ellapsed: 3695.20ms, QPS: 381593.69. (producerCount=4, ringSize=1024, queries=10000000). 
    Benchmark/Multi  Ellapsed: 2990.16ms, QPS: 471568.12. (priorities=[10, 10, 10, 10], ringSize=256x4, queries=10000000).
    Benchmark/Multi  Ellapsed: 2964.46ms, QPS: 475657.41. (priorities=[10, 20, 40, 80], ringSize=256x4, queries=10000000).
    Benchmark/Multi  Ellapsed: 2959.41ms, QPS: 476468.88. (priorities=[2, 8, 32, 80], ringSize=256x4, queries=10000000).
    Benchmark/Multi  Ellapsed: 2827.74ms, QPS: 498654.15. (priorities=[100, 100, 100, 100], ringSize=256x4, queries=10000000).
    Benchmark/Multi  Ellapsed: 3042.72ms, QPS: 463422.64. (priorities=[1000, 1000, 1000, 1000], ringSize=256x4, queries=10000000).

