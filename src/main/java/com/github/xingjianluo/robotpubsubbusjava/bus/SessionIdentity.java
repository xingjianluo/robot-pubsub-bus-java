package com.github.xingjianluo.robotpubsubbusjava.bus;

import java.util.Set;

/**
 * 服务端确认的会话身份，携带发布/订阅授权的主题集合；"*" 表示全部主题。
 */
public record SessionIdentity(String clientId,
                              Set<String> publishTopics,
                              Set<String> subscribeTopics) {

    public boolean canPublish(String topic) {
        return publishTopics.contains("*") || publishTopics.contains(topic);
    }

    public boolean canSubscribe(String topic) {
        return subscribeTopics.contains("*") || subscribeTopics.contains(topic);
    }
}
