/**
 *
 * SleepingMultiPriorityWaitStrategy.java
 * @date 14-8-6 下午7:48
 * @author leo [leoyonn@gmail.com]
 * [CopyRight] All Rights Reserved.
 */

package com.lmax.disruptor;

import java.util.concurrent.locks.LockSupport;

/**
 * @author leo
 */
public class SleepingMultiPriorityWaitStrategy extends MultiPriorityWaitStrategy {

    private static final int RETRIES = 200;
    private volatile int counter = RETRIES;

    @Override
    public void waitForAll() {
        if (counter > 100) {
            --counter;
        } else if (counter > 0) {
            --counter;
            Thread.yield();
        } else {
            LockSupport.parkNanos(1L);
        }
    }

    @Override
    public void signalAllWhenBlocking() {
        counter = RETRIES;
    }
}
