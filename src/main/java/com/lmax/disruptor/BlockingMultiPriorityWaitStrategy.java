/**
 *
 * BlockingMultiPriorityWaitStrategy.java
 * @date 14-8-6 下午7:47
 * @author leo [leoyonn@gmail.com]
 * [CopyRight] All Rights Reserved.
 */

package com.lmax.disruptor;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Blocking wait strategy for multi-priority.
 *
 * @author leo
 */
public class BlockingMultiPriorityWaitStrategy extends MultiPriorityWaitStrategy implements SetTimeout {
    private final Lock lock = new ReentrantLock();
    private final Condition processorNotifyCondition = lock.newCondition();
    protected long timeoutInNanos = -1;
    private final AtomicBoolean signaled = new AtomicBoolean(false);

    @Override
    public void signalAllWhenBlocking() {
        lock.lock();
        try {
            signaled.set(true);
            processorNotifyCondition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void waitForAll() throws InterruptedException {
        lock.lock();
        try {
            if (signaled.get()) {
                signaled.set(false);
                return;
            }
            if (timeoutInNanos > 0) {
                processorNotifyCondition.awaitNanos(timeoutInNanos);
            } else {
                processorNotifyCondition.await();
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void setTimeout(long timeoutInNanos) {
        this.timeoutInNanos = timeoutInNanos;
    }
}
