package com.github.xingjianluo.robotpubsubbusjava.bus;

/**
 * 总线内流转的消息。seq 为主题内单调递增序号，用于有序投递与确认位点。
 */
public record Message(String topic, long seq, byte[] payload, long publishedAtMillis) {
}
