package com.github.xingjianluo.robotpubsubbusjava.bus;

/**
 * 丢弃/断开事件，对客户端可见。
 */
public record DropEvent(String sessionId, String topic, long seq, DropReason reason, String detail) {
}
