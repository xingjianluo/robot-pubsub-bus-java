package com.github.xingjianluo.robotpubsubbusjava.bus.model;

/**
 * 总线上的一条不可变消息（机器人指令或遥测）。
 * seq 为主题内单调递增序号（从 1 开始）。
 */
public record Message(String topic, long seq, String producerPrincipal, byte[] payload) {

    public int payloadSize() {
        return payload == null ? 0 : payload.length;
    }
}
