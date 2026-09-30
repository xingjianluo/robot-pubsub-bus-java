package com.github.xingjianluo.robotpubsubbusjava.bus;

/**
 * 消息总线资源上限与超时配置。
 *
 * @param maxBacklogPerSubscription 单个订阅的最大积压消息数，达到上限后按订阅选择的策略处理
 * @param maxBacklogPerSession      单个会话全部订阅合计可占用的缓冲上限（待投递 + 已投递未确认），
 *                                  不随订阅主题数增多而无界增长
 * @param ackTimeoutMillis          投递后等待确认的超时时间，超时未确认的消息将被重投
 * @param maxPayloadBytes           单条消息负载的最大字节数，超限报文被拒绝
 * @param maxDropsBeforeDisconnect  DROP_OLDEST 策略下连续丢弃达到该次数后，订阅被摘除
 */
public record BusConfig(int maxBacklogPerSubscription,
                        int maxBacklogPerSession,
                        long ackTimeoutMillis,
                        int maxPayloadBytes,
                        int maxDropsBeforeDisconnect) {

    /** 兼容旧用法：不显式指定会话级上限时，取单订阅上限的 4 倍。 */
    public BusConfig(int maxBacklogPerSubscription,
                     long ackTimeoutMillis,
                     int maxPayloadBytes,
                     int maxDropsBeforeDisconnect) {
        this(maxBacklogPerSubscription,
                Math.max(maxBacklogPerSubscription, maxBacklogPerSubscription * 4),
                ackTimeoutMillis,
                maxPayloadBytes,
                maxDropsBeforeDisconnect);
    }

    public static BusConfig defaults() {
        return new BusConfig(100, 400, 5_000L, 65_536, 3);
    }
}
