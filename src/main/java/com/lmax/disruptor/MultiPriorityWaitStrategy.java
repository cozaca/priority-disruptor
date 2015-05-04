/**
 * SkipWaitStrategy.java
 * [CopyRight]
 * @author leo [leoyonn@gmail.com]
 * @date Apr 9, 2014 4:59:39 PM
 */
package com.lmax.disruptor;

/**
 * Just return whether or not this sequence is available.
 *
 * @author leo
 */
public abstract class MultiPriorityWaitStrategy implements WaitStrategy {

    @Override
    public long waitFor(long sequence, Sequence cursor, Sequence dependentSequence, SequenceBarrier barrier)
            throws AlertException, InterruptedException, TimeoutException {
        long availableSequence = dependentSequence.get();
        if ((availableSequence) < sequence) {
            barrier.checkAlert();
        }
        return availableSequence;
    }

    public abstract void waitForAll() throws InterruptedException;
}
