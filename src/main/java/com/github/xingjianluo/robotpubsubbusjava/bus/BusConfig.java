package com.github.xingjianluo.robotpubsubbusjava.bus;

/**
 * 消息总线资源上限与超时配置。
 *
 * @param maxBacklogPerSubscription 单个订阅的最大积压消息数，达到上限后按策略丢弃最旧消息
 * @param ackTimeoutMillis          投递后等待确认的超时时间，超时未确认的消息将被重投
 * @param maxPayloadBytes           单条消息负载的最大字节数，超限报文被拒绝
 * @param maxDropsBeforeDisconnect  订阅连续被丢弃消息达到该次数后，订阅被判定为慢消费者并断开
 */
public record BusConfig(int maxBacklogPerSubscription,
                        long ackTimeoutMillis,
                        int maxPayloadBytes,
                        int maxDropsBeforeDisconnect) {

    public static BusConfig defaults() {
        return new BusConfig(100, 5_000L, 65_536, 3);
    }
}
