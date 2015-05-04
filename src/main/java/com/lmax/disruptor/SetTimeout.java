/**
 *
 * SetTimeout.java
 * @date 14-8-25 下午3:43
 * @author leo [leoyonn@gmail.com]
 * [CopyRight] All Rights Reserved.
 */

package com.lmax.disruptor;

/**
 * Set timeout for next #wait of WaitStrategy.
 *
 * @author leo
 */
public interface SetTimeout {
    /**
     * Mark next #wait to timeoutInNanos.
     *
     * @param timeoutInNanos
     */
    void setTimeout(long timeoutInNanos);
}
