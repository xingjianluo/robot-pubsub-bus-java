package com.github.xingjianluo.robotpubsubbusjava;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * 消息总线配置。令牌表用于本地部署的会话身份核验：
 * 每条记录把一个本地令牌映射为服务端确认的身份与发布/订阅权限，
 * 不依赖外部认证服务、不使用真实凭据。
 */
@ConfigurationProperties(prefix = "bus")
public class BusProperties {

    private int maxBacklogPerSubscription = 100;
    private long ackTimeoutMillis = 5_000L;
    private int maxPayloadBytes = 65_536;
    private int maxDropsBeforeDisconnect = 3;
    /** 单会话全部订阅积压之和的总上限 */
    private int maxBacklogPerSession = 1_000;
    /** 单次拉取允许的最长等待毫秒数 */
    private long maxPollWaitMillis = 1_000L;
    /** 启动时注册的主题 */
    private List<String> topics = new ArrayList<>(List.of("robot.commands", "robot.telemetry"));
    /** 本地令牌表：令牌 → 服务端确认的身份与权限 */
    private List<TokenIdentity> tokens = new ArrayList<>();

    public static class TokenIdentity {
        private String token;
        private String clientId;
        private Set<String> publishTopics = Set.of();
        private Set<String> subscribeTopics = Set.of();

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

        public Set<String> getPublishTopics() {
            return publishTopics;
        }

        public void setPublishTopics(Set<String> publishTopics) {
            this.publishTopics = publishTopics;
        }

        public Set<String> getSubscribeTopics() {
            return subscribeTopics;
        }

        public void setSubscribeTopics(Set<String> subscribeTopics) {
            this.subscribeTopics = subscribeTopics;
        }
    }

    public int getMaxBacklogPerSubscription() {
        return maxBacklogPerSubscription;
    }

    public void setMaxBacklogPerSubscription(int maxBacklogPerSubscription) {
        this.maxBacklogPerSubscription = maxBacklogPerSubscription;
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

    public int getMaxBacklogPerSession() {
        return maxBacklogPerSession;
    }

    public void setMaxBacklogPerSession(int maxBacklogPerSession) {
        this.maxBacklogPerSession = maxBacklogPerSession;
    }

    public long getMaxPollWaitMillis() {
        return maxPollWaitMillis;
    }

    public void setMaxPollWaitMillis(long maxPollWaitMillis) {
        this.maxPollWaitMillis = maxPollWaitMillis;
    }

    public List<String> getTopics() {
        return topics;
    }

    public void setTopics(List<String> topics) {
        this.topics = topics;
    }

    public List<TokenIdentity> getTokens() {
        return tokens;
    }

    public void setTokens(List<TokenIdentity> tokens) {
        this.tokens = tokens;
    }
}
