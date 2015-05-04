/**
 * ParkWaitStrategy.java
 * @date 14-11-28 上午11:49
 * @author leo [leoyonn@gmail.com]
 * [CopyRight] All Rights Reserved.
 */

package com.lmax.disruptor;

import java.util.concurrent.locks.LockSupport;

/**
 * @author leo
 */
public class ParkWaitStrategy implements WaitStrategy {

    private final long nanos;

    public ParkWaitStrategy(long millis) {
        this.nanos = millis * (long) 1e6;
    }

    @Override
    public long waitFor(final long sequence, Sequence cursor, final Sequence dependentSequence, final SequenceBarrier barrier)
            throws AlertException, InterruptedException {

        long availableSequence;
        while ((availableSequence = dependentSequence.get()) < sequence) {
            barrier.checkAlert();
            LockSupport.parkNanos(nanos);
        }

        return availableSequence;
    }

    @Override
    public void signalAllWhenBlocking() {
    }
}
