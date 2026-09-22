package com.github.xingjianluo.robotpubsubbusjava.bus.model;

/** 发布结果：主题内分配的全局序号。 */
public record PublishResult(String topic, long seq) {
}
