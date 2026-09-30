package com.github.xingjianluo.robotpubsubbusjava.web;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * 总线可配置项（bus.* 前缀）。
 */
@ConfigurationProperties(prefix = "bus")
public class BusProperties {

    /** 单订阅积压上限。 */
    private int maxBacklogPerSubscription = 100;
    /** 单会话跨订阅的总缓冲上限（待投递 + 未确认）。 */
    private int maxBacklogPerSession = 400;
    /** 确认超时（毫秒）。 */
    private long ackTimeoutMillis = 5_000L;
    /** 单条报文字节上限。 */
    private int maxPayloadBytes = 65_536;
    /** DROP_OLDEST 下连续丢弃多少次后摘除订阅。 */
    private int maxDropsBeforeDisconnect = 3;
    /** 服务端长轮询最多等待毫秒数（慢消费者只在自己的等待上阻塞）。 */
    private long pollWaitMillis = 5_000L;
    /** 启动时注册的主题。 */
    private List<String> topics = new ArrayList<>();
    /** 服务端登记的客户端凭据（令牌 -> 身份与授权主题）。 */
    private List<ClientProps> clients = new ArrayList<>();

    public int getMaxBacklogPerSubscription() {
        return maxBacklogPerSubscription;
    }

    public void setMaxBacklogPerSubscription(int value) {
        this.maxBacklogPerSubscription = value;
    }

    public int getMaxBacklogPerSession() {
        return maxBacklogPerSession;
    }

    public void setMaxBacklogPerSession(int value) {
        this.maxBacklogPerSession = value;
    }

    public long getAckTimeoutMillis() {
        return ackTimeoutMillis;
    }

    public void setAckTimeoutMillis(long ackTimeoutMillis) {
        this.ackTimeoutMillis = ackTimeoutMillis;
    }

    public int getMaxPayloadBytes() {
        return maxPayloadBytes;
    }

    public void setMaxPayloadBytes(int maxPayloadBytes) {
        this.maxPayloadBytes = maxPayloadBytes;
    }

    public int getMaxDropsBeforeDisconnect() {
        return maxDropsBeforeDisconnect;
    }

    public void setMaxDropsBeforeDisconnect(int maxDropsBeforeDisconnect) {
        this.maxDropsBeforeDisconnect = maxDropsBeforeDisconnect;
    }

    public long getPollWaitMillis() {
        return pollWaitMillis;
    }

    public void setPollWaitMillis(long pollWaitMillis) {
        this.pollWaitMillis = pollWaitMillis;
    }

    public List<String> getTopics() {
        return topics;
    }

    public void setTopics(List<String> topics) {
        this.topics = topics;
    }

    public List<ClientProps> getClients() {
        return clients;
    }

    public void setClients(List<ClientProps> clients) {
        this.clients = clients;
    }

    /** 单个登记客户端的配置。 */
    public static class ClientProps {
        private String token;
        private String clientId;
        private List<String> publishTopics = new ArrayList<>();
        private List<String> subscribeTopics = new ArrayList<>();

        public String getToken() {
            return token;
        }

        public void setToken(String token) {
            this.token = token;
        }

        public String getClientId() {
            return clientId;
        }

        public void setClientId(String clientId) {
            this.clientId = clientId;
        }

        public List<String> getPublishTopics() {
            return publishTopics;
        }

        public void setPublishTopics(List<String> publishTopics) {
            this.publishTopics = publishTopics;
        }

        public List<String> getSubscribeTopics() {
            return subscribeTopics;
        }

        public void setSubscribeTopics(List<String> subscribeTopics) {
            this.subscribeTopics = subscribeTopics;
        }
    }
}
