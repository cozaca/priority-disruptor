/**
 *
 * SleepingMultiPriorityWaitStrategy.java
 * @date 14-8-6
 * @author leo [leoyonn@gmail.com]
 * [CopyRight] All Rights Reserved.
 */

package com.lmax.disruptor;

import java.util.concurrent.locks.LockSupport;

/**
 * @author leo
 */
public class ParkMultiPriorityWaitStrategy extends MultiPriorityWaitStrategy {

    private final long nanos;

    public ParkMultiPriorityWaitStrategy(long millis) {
        this.nanos = millis * (long) 1e6;
    }

    @Override
    public void waitForAll() {
        LockSupport.parkNanos(nanos);
    }

    @Override
    public void signalAllWhenBlocking() {
    }
}
