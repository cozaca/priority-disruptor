/**
 * PriorityConfig.java
 * [CopyRight]
 * @author leo [leoyonn@gmail.com]
 * @date Apr 8, 2014 7:39:32 PM
 */
package com.lmax.disruptor;

/**
 * Configuration for an priority.
 * 
 * @author leo
 */
public class PriorityConfig {
    public static final int DEFAULT_MAX_BATCH = 1 >> 10;
    private long curSeq;
    private int batchPerRound;

    public PriorityConfig(int batchPerRound) {
        this.batchPerRound = batchPerRound;
    }

    public long curSeq() {
        return curSeq;
    }

    public int batchPerRound() {
        return batchPerRound;
    }

    public PriorityConfig curSeq(long seq) {
        this.curSeq = seq;
        return this;
    }

    /**
     * Create an array of priority configurations by #batchPerRound.
     *
     * @param batchPerRound :<B>batchPerRound should be in descending order.</B>
     * @return
     */
    public static PriorityConfig[] create(int... batchPerRound) {
        if (batchPerRound == null) {
            return new PriorityConfig[]{
                    new PriorityConfig(DEFAULT_MAX_BATCH)
            };
        }
        PriorityConfig[] configs = new PriorityConfig[batchPerRound.length];
        int last = Integer.MAX_VALUE;
        for (int i = 0; i < batchPerRound.length; i++) {
            int batch = batchPerRound[i];
            if (batch <= 0 || batch > last) {
                throw new IllegalArgumentException("Batch-per-round invalid: should be positive and in descending order");
            }
            configs[i] = new PriorityConfig(batch);
            last = batch;
        }
        return configs;
    }
}
