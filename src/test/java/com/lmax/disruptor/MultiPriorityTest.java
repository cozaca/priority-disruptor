/**
 * MultiPriorityTest.java
 * [CopyRight]
 * @author leo [leoyonn@gmail.com]
 * @date Apr 8, 2014 9:14:34 PM
 */
package com.lmax.disruptor;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.MultiPriorityDisruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author leo
 */
public class MultiPriorityTest {
    MultiPriorityDisruptor<Event> multiDisruptor;
    Disruptor<Event> singleDisruptor;
    RingBuffer<Event> singleRingBuffer;
    AtomicInteger[] indexers;

    class Event {
        int priority;
        int idx;
        String value;

        Event set(int priority, int idx, String v) {
            this.priority = priority;
            this.idx = idx;
            this.value = v;
            return this;
        }

        @Override
        public String toString() {
            return "P" + priority + "-" + idx + ":" + value;
        }
    }

    class EventFactory implements com.lmax.disruptor.EventFactory<Event> {
        @Override
        public Event newInstance() {
            return new Event();
        }
    }

    class Handler implements EventHandler<Event> {
        @Override
        public void onEvent(Event event, long sequence, boolean endOfBatch) throws Exception {
            AtomicInteger i = indexers[event.priority];
            Assert.assertEquals(i.get() + 1, event.idx);
            i.set(event.idx);
        }
    }

    void print(Object... objs) {
        for (Object o : objs) {
            System.out.println(o);
        }
    }

    void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }
    }

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

    boolean publishSingle(boolean sync, int priority, int idx, String value) {
        long sequence = 0;
        if (sync) {
            sequence = singleRingBuffer.next();
        } else {
            try {
                sequence = singleRingBuffer.tryNext();
            } catch (InsufficientCapacityException ex) {
                return false;
            }
        }
        try {
            singleRingBuffer.get(sequence).set(priority, idx, value);
        } finally {
            singleRingBuffer.publish(sequence);
        }
        return true;
    }

    enum Type {
        Single, Multi,
    }

    class Task implements Runnable {
        final Type type;
        final int priority;
        final int N;

        public Task(Type type, int priority, int N) {
            this.type = type;
            this.priority = priority;
            this.N = N;
        }

        @Override
        public void run() {
            for (int i = 0; i < N; i++) {
                if (type == Type.Multi) {
                    publishMulti(true, priority, i, "这是一个P" + priority + "的消息-" + i);
                } else {
                    publishSingle(true, priority, i, "这是一个P" + priority + "的消息-" + i);
                }
            }
        }
    }

    private void benchmarkMulti(int N, int[] priorities, int ringSize) throws InterruptedException {
        int p = priorities.length;
        indexers = new AtomicInteger[p];
        for (int i = 0; i < p; i++) {
            indexers[i] = new AtomicInteger(-1);
        }
        Thread[] publishers = new Thread[p];
        for (int i = 0; i < p; i++) {
            publishers[i] = new Thread(new Task(Type.Multi, i, N));
        }

        ExecutorService executor = Executors.newSingleThreadExecutor();
        multiDisruptor = new MultiPriorityDisruptor<Event>(new EventFactory(), ringSize, executor, ProducerType.MULTI,
                new BlockingMultiPriorityWaitStrategy(), priorities);
        multiDisruptor.handleEventsWith(new Handler());
        multiDisruptor.start();
        long begin = System.nanoTime();
        for (int i = 0; i < p; i++) {
            publishers[i].start();
        }
        for (int i = 0; i < p; i++) {
            publishers[i].join();
        }
        for (int i = 0; i < p; i++) {
            while (N - 1 != indexers[i].get()) {
            }
        }
        double elapsed = (System.nanoTime() - begin) / 1e6;
        double qps = 1000 * N / elapsed;
        print(String.format("Benchmark/Multi  Ellapsed: %.2fms, QPS: %.2f. (priorities=%s, ringSize=%dx%d, queries=%d).",
                elapsed, qps, Arrays.toString(priorities), ringSize, priorities.length, N));
        Thread.sleep(100);
    }


    private void benchmarkSingle(int N, int producerCount, int ringSize) throws InterruptedException {
        indexers = new AtomicInteger[producerCount];
        for (int i = 0; i < producerCount; i++) {
            indexers[i] = new AtomicInteger(-1);
        }
        Thread[] publishers = new Thread[producerCount];
        for (int i = 0; i < producerCount; i++) {
            publishers[i] = new Thread(new Task(Type.Single, i, N));
        }

        ExecutorService executor = Executors.newSingleThreadExecutor();
        singleDisruptor = new Disruptor<Event>(new EventFactory(), ringSize, executor,
                ProducerType.MULTI, new BoundedTimeoutBlockingWaitStrategy());
        singleDisruptor.handleEventsWith(new Handler());
        singleRingBuffer = singleDisruptor.start();
        long begin = System.nanoTime();
        for (int i = 0; i < producerCount; i++) {
            publishers[i].start();
        }
        for (int i = 0; i < producerCount; i++) {
            publishers[i].join();
        }
        for (int i = 0; i < producerCount; i++) {
            while (N - 1 != indexers[i].get()) {
            }
        }
        double elapsed = (System.nanoTime() - begin) / 1e6;
        double qps = 1000 * N / elapsed;
        print(String.format("Benchmark/Single Ellapsed: %.2fms, QPS: %.2f. (producerCount=%d, ringSize=%d, queries=%d). ",
                elapsed, qps, producerCount, ringSize, N));
        Thread.sleep(100);
    }

    @Test
    public void bentchmark() throws InterruptedException {
        benchmarkSingle(10000000, 1, 1 << 10);
        benchmarkMulti(10000000, new int[]{1000, 1000}, 1 << 10);
    }
}
