/**
 * MultiPriorityProcessor.java
 * [CopyRight]
 * @author leo [leoyonn@gmail.com]
 * @date Apr 8, 2014 4:06:24 PM
 */

package com.lmax.disruptor;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

/**
 * Convenience class for handling the batching semantics of consuming entries from a {@link RingBuffer} and
 * delegating the available events to an {@link EventHandler}.
 * If the {@link EventHandler} also implements {@link LifecycleAware} it will be notified just after the
 * thread is started and just before the thread is shutdown.
 * 
 * @param <T> event implementation storing the data for sharing during exchange or parallel coordination of an
 *            event.
 * @author leo
 */
public class MultiPriorityProcessor<T> implements EventProcessor {
    private static final Logger LOGGER = Logger.getLogger(MultiPriorityProcessor.class.getName());

    private final AtomicBoolean running = new AtomicBoolean(false);
    private ExceptionHandler exceptionHandler = new FatalExceptionHandler();
    private final int size;
    private final DataProvider<T>[] dataProviders;
    private final SequenceBarrier[] sequenceBarriers;
    private final EventHandler<T> eventHandler;
    private final Sequence[] sequences;
    private final PriorityConfig[] priorityConfigs;
    private final MultiPriorityWaitStrategy waitStrategy;

    /**
     * Construct a {@link EventProcessor} that will automatically track the progress by updating its sequence
     * when the {@link EventHandler#onEvent(Object, long, boolean)} method returns.
     *
     * @param ringBuffers     to which events are published.
     * @param eventHandler    is the delegate to which events are dispatched.
     * @param waitStrategy
     * @param priorityConfigs
     */
    public MultiPriorityProcessor(final RingBuffer<T>[] ringBuffers, final EventHandler<T> eventHandler,
            final MultiPriorityWaitStrategy waitStrategy, PriorityConfig[] priorityConfigs) {
        size = ringBuffers.length;
        this.dataProviders = ringBuffers;
        this.sequenceBarriers = new SequenceBarrier[size];
        this.eventHandler = eventHandler;
        sequences = new Sequence[size];
        this.priorityConfigs = priorityConfigs;
        this.waitStrategy = waitStrategy;
        final Sequence[] EMPTY_SEQUENCES = new Sequence[0];
        for (int i = 0; i < size; i++) {
            sequences[i] = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
            sequenceBarriers[i] = ringBuffers[i].newBarrier(EMPTY_SEQUENCES);
        }
    }

    @Override
    public Sequence getSequence() {
        throw new UnsupportedOperationException("Call getSequence(int) instead!");
    }

    public Sequence getSequence(int i) {
        return sequences[i];
    }

    public Sequence[] sequences() {
        return sequences;
    }

    @Override
    public void halt() {
        running.set(false);
        for (SequenceBarrier barrier : sequenceBarriers) {
            barrier.alert();
        }
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    /**
     * Set a new {@link ExceptionHandler} for handling exceptions propagated out of the
     * {@link BatchEventProcessor}
     * 
     * @param exceptionHandler to replace the existing exceptionHandler.
     */
    public void setExceptionHandler(final ExceptionHandler exceptionHandler) {
        if (null == exceptionHandler) {
            throw new NullPointerException();
        }
        this.exceptionHandler = exceptionHandler;
    }

    /**
     * @param priority
     * @return
     */
    private int process(int priority) {
        T event = null;
        long beginSeq = priorityConfigs[priority].curSeq(), curSeq = beginSeq + 1, availableSeq = beginSeq;
        int batch = priorityConfigs[priority].batchPerRound();
        try {
            availableSeq = sequenceBarriers[priority].waitFor(curSeq);
            if (availableSeq <= beginSeq) {
                return 0;
            }
            while (curSeq <= availableSeq && curSeq - beginSeq <= batch) {
                event = dataProviders[priority].get(curSeq);
                eventHandler.onEvent(event, curSeq, curSeq == availableSeq);
                curSeq++;
            }
            curSeq--;
            sequences[priority].set(curSeq);
            priorityConfigs[priority].curSeq(curSeq);
        } catch (final TimeoutException ex) {
            notifyTimeout(sequences[priority].get());
        } catch (final AlertException ex) {
            if (!running.get()) {
                return -1;
            }
        } catch (final Throwable ex) {
            exceptionHandler.handleEventException(ex, curSeq, event);
            sequences[priority].set(curSeq);
            priorityConfigs[priority].curSeq(curSeq);
        }
        return (int) (curSeq - beginSeq);
    }

    /**
     * It is ok to have another thread rerun this method after a halt().
     * 
     * @throws IllegalStateException if this object instance is already running in a thread
     */
    @Override
    public void run() {
        if (!running.compareAndSet(false, true)) {
            throw new IllegalStateException("Thread is already running");
        }
        for (SequenceBarrier barrier : sequenceBarriers) {
            barrier.clearAlert();
        }
        notifyStart();
        for (int i = 0; i < size; i++) {
            priorityConfigs[i].curSeq(sequences[i].get());
        }

        try {
            while (running.get()) {
                boolean allEmpty = true;
                for (int i = 0; i < size; i++) {
                    if (process(i) > 0) {
                        allEmpty = false;
                    }
                }
                if (allEmpty) {
                    try {
                        waitStrategy.waitForAll();
                    } catch (InterruptedException ex) {
                        LOGGER.warning("Got exception when waitAll: " + ex.getMessage());
                    }
                }
            }
        } finally {
            notifyShutdown();
            running.set(false);
        }
    }

    private void notifyTimeout(final long availableSequence) {
    }

    /**
     * Notifies the EventHandler when this processor is starting up
     */
    private void notifyStart() {
        if (eventHandler instanceof LifecycleAware) {
            try {
                ((LifecycleAware) eventHandler).onStart();
            } catch (final Throwable ex) {
                exceptionHandler.handleOnStartException(ex);
            }
        }
    }

    /**
     * Notifies the EventHandler immediately prior to this processor shutting down
     */
    private void notifyShutdown() {
        if (eventHandler instanceof LifecycleAware) {
            try {
                ((LifecycleAware) eventHandler).onShutdown();
            } catch (final Throwable ex) {
                exceptionHandler.handleOnShutdownException(ex);
            }
        }
    }
}
