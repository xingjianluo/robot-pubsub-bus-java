package com.github.xingjianluo.robotpubsubbusjava.bus;

/**
 * 消息被丢弃或订阅被断开的原因，必须可区分。
 */
public enum DropReason {
    /** 订阅积压达到上限，最旧消息被丢弃 */
    BACKLOG_OVERFLOW,
    /** 慢消费者：连续丢弃次数超限，订阅被断开 */
    SLOW_CONSUMER_EVICTED
}
