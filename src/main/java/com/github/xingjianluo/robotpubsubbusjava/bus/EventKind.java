package com.github.xingjianluo.robotpubsubbusjava.bus;

/**
 * 对客户端可见的投递事件类别，彼此可区分。
 */
public enum EventKind {
    /** 正常投递：客户端收到了这条消息 */
    DELIVERED,
    /** 背压丢弃：fromSeq..toSeq 的消息丢失，形成可识别的序号缺口 */
    GAP,
    /** 订阅被摘除（慢消费者/背压策略触发）；会话保留，重连后可续传 */
    SUBSCRIPTION_EVICTED,
    /** 会话被服务端关闭，不可再使用 */
    SESSION_CLOSED
}
