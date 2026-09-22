package com.github.xingjianluo.robotpubsubbusjava.bus.model;

import java.util.Set;

/**
 * 服务端确认的会话身份。authorization 为不透明令牌，
 * allowedPublishTopics / allowedSubscribeTopics 为授权主题集合（支持后缀 * 通配）。
 */
public record Principal(String name, String token,
                        Set<String> allowedPublishTopics,
                        Set<String> allowedSubscribeTopics) {
}
