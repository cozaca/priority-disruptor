/**
 *
 * WaitStrategyType.java
 * @date 14-8-25 下午3:13
 * @author leo [leoyonn@gmail.com]
 * [CopyRight] All Rights Reserved.
 */

package com.lmax.disruptor;

/**
 * Supported Wait Strategy.
 *
 * @author leo
 */
public enum WaitStrategyType {
    Blocking, Sleeping, Park,
}
