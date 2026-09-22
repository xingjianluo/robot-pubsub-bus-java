package com.github.xingjianluo.robotpubsubbusjava.bus.config;

import java.util.HashMap;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * robot.bus.* 配置。
 * principals: token -> 身份配置（名称/发布主题/订阅主题）。
 * topics: 主题名 -> 主题配置（策略/积压上限/保留窗口/报文字节上限）。
 */
@ConfigurationProperties(prefix = "robot.bus")
public class BusProperties {

    private long ackTimeoutMillis = 5000;
    private int reaperIntervalMillis = 200;
    private Map<String, PrincipalProps> principals = new HashMap<>();
    private Map<String, TopicProps> topics = new HashMap<>();

    public long getAckTimeoutMillis() {
        return ackTimeoutMillis;
    }

    public void setAckTimeoutMillis(long ackTimeoutMillis) {
        this.ackTimeoutMillis = ackTimeoutMillis;
    }

    public int getReaperIntervalMillis() {
        return reaperIntervalMillis;
    }

    public void setReaperIntervalMillis(int reaperIntervalMillis) {
        this.reaperIntervalMillis = reaperIntervalMillis;
    }

    public Map<String, PrincipalProps> getPrincipals() {
        return principals;
    }

    public void setPrincipals(Map<String, PrincipalProps> principals) {
        this.principals = principals;
    }

    public Map<String, TopicProps> getTopics() {
        return topics;
    }

    public void setTopics(Map<String, TopicProps> topics) {
        this.topics = topics;
    }

    /** 身份配置。 */
    public static class PrincipalProps {
        private String name;
        private java.util.List<String> publish = new java.util.ArrayList<>();
        private java.util.List<String> subscribe = new java.util.ArrayList<>();

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public java.util.List<String> getPublish() {
            return publish;
        }

        public void setPublish(java.util.List<String> publish) {
            this.publish = publish;
        }

        public java.util.List<String> getSubscribe() {
            return subscribe;
        }

        public void setSubscribe(java.util.List<String> subscribe) {
            this.subscribe = subscribe;
        }
    }

    /** 主题配置。 */
    public static class TopicProps {
        private String policy = "REJECT";
        private int maxInflight = 16;
        private int retentionSize = 1024;
        private int maxPayloadBytes = 65536;

        public String getPolicy() {
            return policy;
        }

        public void setPolicy(String policy) {
            this.policy = policy;
        }

        public int getMaxInflight() {
            return maxInflight;
        }

        public void setMaxInflight(int maxInflight) {
            this.maxInflight = maxInflight;
        }

        public int getRetentionSize() {
            return retentionSize;
        }

        public void setRetentionSize(int retentionSize) {
            this.retentionSize = retentionSize;
        }

        public int getMaxPayloadBytes() {
            return maxPayloadBytes;
        }

        public void setMaxPayloadBytes(int maxPayloadBytes) {
            this.maxPayloadBytes = maxPayloadBytes;
        }
    }
}
