package com.github.xingjianluo.robotpubsubbusjava.bus;

/**
 * 订阅级背压策略：积压（或会话总量）达到上限时如何处理。
 */
public enum BackpressurePolicy {
    /** 丢弃最旧消息为新消息腾出位置；连续丢弃超限后订阅被摘除 */
    DROP_OLDEST,
    /** 一旦超限即把整个订阅摘除；会话保留，重连后从已确认位点续传 */
    EVICT_SUBSCRIPTION
}
