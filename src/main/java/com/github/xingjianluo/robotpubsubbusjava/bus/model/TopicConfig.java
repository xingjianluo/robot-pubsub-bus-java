package com.github.xingjianluo.robotpubsubbusjava.bus.model;

/**
 * 主题配置：背压策略、单订阅者最大未确认积压、主题保留消息数、单条报文字节上限。
 */
public record TopicConfig(String name,
                         BackpressurePolicy backpressurePolicy,
                         int maxInflight,
                         int retentionSize,
                         int maxPayloadBytes) {
}
