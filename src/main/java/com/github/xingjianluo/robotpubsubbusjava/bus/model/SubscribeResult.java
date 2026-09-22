package com.github.xingjianluo.robotpubsubbusjava.bus.model;

/** 订阅结果：订阅标识与当前已确认位点（断线重连时大于 0）。 */
public record SubscribeResult(String subscriptionId, long committedSeq, boolean resumed) {
}
