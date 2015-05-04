/**
 * BoundedTimeoutBlockingWaitStrategy.java
 * @date 14-8-25 下午3:23
 * @author leo [leoyonn@gmail.com]
 * [CopyRight] All Rights Reserved.
 */

package com.lmax.disruptor;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Blocking wait strategy that can update timeoutInNanos at run.
 *
 * @author leo
 */
public class BoundedTimeoutBlockingWaitStrategy implements WaitStrategy, SetTimeout {
    private final Lock lock = new ReentrantLock();
    private final Condition processorNotifyCondition = lock.newCondition();
    protected long timeoutInNanos = -1;

    @Override
    public long waitFor(final long sequence, final Sequence cursorSequence, final Sequence dependentSequence,
            final SequenceBarrier barrier) throws AlertException, InterruptedException, TimeoutException {

        long availableSequence;
        if ((availableSequence = cursorSequence.get()) < sequence) {
            lock.lock();
            try {
                if ((availableSequence = cursorSequence.get()) < sequence) {
                    barrier.checkAlert();
                    if (timeoutInNanos > 0) {
                        processorNotifyCondition.awaitNanos(timeoutInNanos);
                    } else {
                        processorNotifyCondition.await();
                    }
                }
            } finally {
                lock.unlock();
            }
        }

        return availableSequence;
    }

    @Override
    public void signalAllWhenBlocking() {
        lock.lock();
        try {
            processorNotifyCondition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void setTimeout(long timeoutInNanos) {
        this.timeoutInNanos = timeoutInNanos;
    }
}