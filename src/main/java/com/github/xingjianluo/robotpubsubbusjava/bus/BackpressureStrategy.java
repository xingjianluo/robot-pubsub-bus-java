package com.github.xingjianluo.robotpubsubbusjava.bus;

/**
 * 订阅积压达到上限时的处理策略，按订阅在订阅时选择。
 */
public enum BackpressureStrategy {
    /** 丢弃最旧消息以腾出纳新空间，被丢弃的序号形成可识别缺口并以事件告知客户端 */
    DROP_OLDEST,
    /** 直接把整个订阅摘除；会话保留，重连后订阅重新挂上并从已确认位点续收 */
    EVICT_SUBSCRIPTION
}
