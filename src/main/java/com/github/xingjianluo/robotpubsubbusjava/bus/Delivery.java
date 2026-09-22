package com.github.xingjianluo.robotpubsubbusjava.bus;

/**
 * 一次投递：redelivered 区分首次投递与重投，attempt 为投递次数（从 1 开始）。
 */
public record Delivery(Message message, boolean redelivered, int attempt) {
}
