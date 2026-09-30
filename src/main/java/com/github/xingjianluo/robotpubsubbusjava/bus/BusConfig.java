package com.github.xingjianluo.robotpubsubbusjava.bus;

/**
 * 消息总线资源上限与超时配置。
 *
 * @param maxBacklogPerSubscription 单个订阅的最大积压消息数，达到上限后按订阅的背压策略处理
 * @param ackTimeoutMillis          投递后等待确认的超时时间，超时未确认的消息将被重投
 * @param maxPayloadBytes           单条消息负载的最大字节数，超限报文被拒绝
 * @param maxDropsBeforeDisconnect  订阅连续被丢弃消息达到该次数后，订阅被判定为慢消费者并断开
 * @param maxBacklogPerSession      单个会话全部订阅积压之和的总上限，不随订阅数无界增长
 * @param maxPollWaitMillis         单次拉取允许的最长等待时间，慢会话只在自己的等待里有界阻塞
 */
public record BusConfig(int maxBacklogPerSubscription,
                        long ackTimeoutMillis,
                        int maxPayloadBytes,
                        int maxDropsBeforeDisconnect,
                        int maxBacklogPerSession,
                        long maxPollWaitMillis) {

    /** 兼容旧构造：会话总上限默认为单订阅上限的 10 倍，拉取等待上限 1 秒。 */
    public BusConfig(int maxBacklogPerSubscription,
                     long ackTimeoutMillis,
                     int maxPayloadBytes,
                     int maxDropsBeforeDisconnect) {
        this(maxBacklogPerSubscription, ackTimeoutMillis, maxPayloadBytes,
                maxDropsBeforeDisconnect, maxBacklogPerSubscription * 10, 1_000L);
    }

    public static BusConfig defaults() {
        return new BusConfig(100, 5_000L, 65_536, 3, 1_000, 1_000L);
    }
}
