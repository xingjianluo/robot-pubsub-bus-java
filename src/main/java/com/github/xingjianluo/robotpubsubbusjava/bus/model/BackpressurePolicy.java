package com.github.xingjianluo.robotpubsubbusjava.bus.model;

/** 单个订阅者积压达到上限时的确定策略。 */
public enum BackpressurePolicy {
    /** 拒绝本次发布，向发布者返回可区分错误。 */
    REJECT,
    /** 丢弃该订阅者最旧的未确认消息（对其产生可解释缺口），发布不阻塞。 */
    DROP_OLDEST,
    /** 断开慢订阅者连接，给出可区分原因，允许其重连续传。 */
    DISCONNECT
}
