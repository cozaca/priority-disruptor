/**
 * MultiPriorityDisruptor.java
 * [CopyRight]
 * @author leo [leoyonn@gmail.com]
 * @date Apr 8, 2014 7:11:34 PM
 */
package com.lmax.disruptor.dsl;

import java.util.Arrays;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.lmax.disruptor.*;
import com.lmax.disruptor.MultiPriorityWaitStrategy;

import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.BasicExecutor;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import java.util.Arrays;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A {@link Disruptor} api with multiple priority(ring-buffer).
 * Can't extends Disruptor because private constructor...
 *
 * @param <T> the type of event used.
 * @author leo
 */
public class MultiPriorityDisruptor<T> {
    private final RingBuffer<T>[] ringBuffers;
    private final PriorityConfig[] priorityConfigs;
    private final int size;
    private final  Executor executor;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final MultiPriorityWaitStrategy waitStrategy;
    private MultiPriorityProcessor<T> processor;

    /**
     * Create a new MultiPriorityDisruptor.
     *
     * @param eventFactory   the factory to create events in the ring buffer.
     * @param ringBufferSize same size for all the ring-buffers.
     * @param threadFactory       an {@link ThreadFactory} to execute event processors.
     * @param producerType   the claim strategy to use for the ring buffer.
     * @param waitStrategy   the wait strategy should be instanceof {@link MultiPriorityWaitStrategy}
     * @param batchPerRounds batch-per-round for each priority. <B>batchPerRound should be in descending order.</B>
     */
    public MultiPriorityDisruptor(final EventFactory<T> eventFactory, final int ringBufferSize,
                                  final ThreadFactory threadFactory, final ProducerType producerType, final MultiPriorityWaitStrategy waitStrategy,
                                  final int... batchPerRounds) {
        this(eventFactory, sameRingBufferSizes((batchPerRounds == null || batchPerRounds.length <= 1) ?
                        1 : batchPerRounds.length, ringBufferSize),
                new BasicExecutor(threadFactory), producerType, waitStrategy, batchPerRounds
        );
    }

    public MultiPriorityDisruptor(final EventFactory<T> eventFactory, final int ringBufferSize,
                                  final Executor executor, final ProducerType producerType, final MultiPriorityWaitStrategy waitStrategy,
                                  final int... batchPerRounds) {
        this(eventFactory, sameRingBufferSizes((batchPerRounds == null || batchPerRounds.length <= 1) ?
                        1 : batchPerRounds.length, ringBufferSize),
                executor, producerType, waitStrategy, batchPerRounds
        );
    }

    /**
     * Create a new MultiPriorityDisruptor.
     *
     * @param eventFactory    the factory to create events in the ring buffer.
     * @param ringBufferSizes size the ring-buffers.
     * @param threadFactory        an {@link ThreadFactory} to execute event processors.
     * @param producerType    the claim strategy to use for the ring buffer.
     * @param waitStrategy    the wait strategy should be instanceof {@link MultiPriorityWaitStrategy}
     * @param batchPerRounds  batch-per-round for each priority. <B>batchPerRound should be in descending order.</B>
     */
    public MultiPriorityDisruptor(final EventFactory<T> eventFactory, final int[] ringBufferSizes,
                                  final Executor executor, final ProducerType producerType, final MultiPriorityWaitStrategy waitStrategy,
                                  final int... batchPerRounds) {
        if (ringBufferSizes.length != batchPerRounds.length) {
            throw new IllegalArgumentException("ring buffer size should be same as batch-per-rounds size");
        }
        this.priorityConfigs = PriorityConfig.create(batchPerRounds);
        this.size = priorityConfigs.length;
        //noinspection unchecked
        this.ringBuffers = (RingBuffer<T>[]) new RingBuffer<?>[size];
        this.executor = executor;
        this.waitStrategy = waitStrategy;
        for (int i = 0; i < size; i++) {
            ringBuffers[i] = RingBuffer.create(producerType, eventFactory, ringBufferSizes[i], waitStrategy);
        }
    }

    private static int[] sameRingBufferSizes(int count, int size) {
        int[] sizes = new int[count];
        Arrays.fill(sizes, size);
        return sizes;
    }

    /**
     * Set up event handler to handle events from the priority-th ring buffer.
     *
     * @param handler the event handlers that will process events.
     */
    public void handleEventsWith(final EventHandler<T> handler) {
        if (started.get()) {
            throw new IllegalStateException("All event handlers must be added before calling starts.");
        }
        processor = new MultiPriorityProcessor<T>(ringBuffers, handler, waitStrategy, priorityConfigs);
    }



    /**
     * Publish an event to the ring buffer.
     *
     * @param eventTranslator the translator that will load data into the event.
     */
    public void publishEvent(final int priority, final EventTranslator<T> eventTranslator) {
        ringBuffers[priority].publishEvent(eventTranslator);
    }

    /**
     * Publish an event to the ring buffer.
     *
     * @param eventTranslator the translator that will load data into the event.
     * @param arg A single argument to load into the event
     */
    public <A> void publishEvent(final int priority, final EventTranslatorOneArg<T, A> eventTranslator, A arg) {
        ringBuffers[priority].publishEvent(eventTranslator, arg);
    }

    /**
     * Publish a batch of events to the ring buffer.
     *
     * @param eventTranslator the translator that will load data into the event.
     * @param arg An array single arguments to load into the events. One Per event.
     */
    public <A> void publishEvents(final int priority, final EventTranslatorOneArg<T, A> eventTranslator, A[] arg) {
        ringBuffers[priority].publishEvents(eventTranslator, arg);
    }

    /**
     * <p>
     * Starts the event processors and returns the fully configured ring buffer.
     * </p>
     * <p>
     * The ring buffer is set up to prevent overwriting any entry that is yet to be processed by the slowest
     * event processor.
     * </p>
     * <p>
     * This method must only be called once after all event processors have been added.
     * </p>
     *
     * @return this started disruptor.
     */
    public MultiPriorityDisruptor<T> start() {
        if (!started.compareAndSet(false, true)) {
            throw new IllegalStateException("Disruptor.start() must only be called once.");
        }
        if (processor == null) {
            throw new IllegalStateException("Disruptor.handleEventsWith() should be called before start.");
        }
        for (int i = 0; i < size; i++) {
            ringBuffers[i].addGatingSequences(processor.getSequence(i));
        }
        executor.execute(processor);
        return this;
    }

    /**
     * Calls {@link com.lmax.disruptor.EventProcessor#halt()} on all of the event processors created via this
     * disruptor.
     */
    public void halt() {
        processor.halt();
    }

    /**
     * Waits until all events currently in the disruptor have been processed by all event processors
     * and then halts the processors. It is critical that publishing to the ring buffer has stopped
     * before calling this method, otherwise it may never return.
     * <p>
     * This method will not shutdown the executor, nor will it await the final termination of the processor
     * threads.
     * </p>
     */
    public void shutdown() throws TimeoutException {
        shutdown(-1, TimeUnit.MILLISECONDS);
    }

    /**
     * <p>
     * Waits until all events currently in the disruptor have been processed by all event processors and then
     * halts the processors.
     * </p>
     * <p>
     * This method will not shutdown the executor, nor will it await the final termination of the processor
     * threads.
     * </p>
     *
     * @param timeout the amount of time to wait for all events to be processed. <code>-1</code> will give an
     *            infinite timeout
     * @param timeUnit the unit the timeOut is specified in
     */
    public void shutdown(final long timeout, final TimeUnit timeUnit) throws TimeoutException {
        long timeOutAt = System.currentTimeMillis() + timeUnit.toMillis(timeout);
        while (true) {
            for (int i = 0; i < size; i++) {
                if (!hasBacklog(i)) {
                    halt();
                    return;
                }
                if (timeout >= 0 && System.currentTimeMillis() > timeOutAt) {
                    throw TimeoutException.INSTANCE;
                }
                // Busy spin
            }
        }
    }

    /**
     * The priority-th {@link RingBuffer} used by this Disruptor. This is useful for creating custom
     * event processors if the behaviour of {@link BatchEventProcessor} is not suitable.
     *
     * @return the ring buffer used by this Disruptor.
     */
    public RingBuffer<T> getRingBuffer(final int priority) {
        return ringBuffers[priority];
    }

    /**
     * Get the value of the cursor indicating the published sequence.
     *
     * @return value of the cursor for events that have been published.
     */
    public long getCursor(final int priority) {
        return ringBuffers[priority].getCursor();
    }

    /**
     * The capacity of the data structure to hold entries.
     *
     * @return the size of the RingBuffer.
     * @see com.lmax.disruptor.Sequencer#getBufferSize()
     */
    public long getBufferSize(final int priority) {
        return ringBuffers[priority].getBufferSize();
    }

    /**
     * Get the event for a given sequence in the RingBuffer.
     *
     * @param sequence for the event.
     * @return event for the sequence.
     * @see RingBuffer#get(long)
     */
    public T get(final int priority, final long sequence) {
        return ringBuffers[priority].get(sequence);
    }

    /**
     * Confirms if all messages have been consumed by all event processors.
     */
    private boolean hasBacklog(int priority) {
        final long cursor = ringBuffers[priority].getCursor();
        for (Sequence seq : processor.sequences()) {
            if (cursor > seq.get()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Get next sequence of this priority.
     *
     * @param priority
     * @return
     */
    public long next(int priority) {
        return ringBuffers[priority].next();
    }

    /**
     * Try to get next sequence of this priority.
     *
     * @param priority
     * @return
     * @throws InsufficientCapacityException
     */
    public long tryNext(int priority) throws InsufficientCapacityException {
        return ringBuffers[priority].tryNext();
    }

    /**
     * Publish the specified sequence of this priority.
     *
     * @param priority
     * @param sequence
     */
    public void publish(int priority, long sequence) {
        ringBuffers[priority].publish(sequence);
    }

    /**
     * Get the remaining capacity of ring buffer of this priority.
     *
     * @param priority
     * @return
     */
    public long remains(int priority) {
        return ringBuffers[priority].remainingCapacity();
    }

    /**
     * Get the occupied size of ring buffer of this priority.
     *
     * @param priority
     * @return
     */
    public long occupied(int priority) {
        return ringBuffers[priority].getBufferSize() - ringBuffers[priority].remainingCapacity();
    }
}
