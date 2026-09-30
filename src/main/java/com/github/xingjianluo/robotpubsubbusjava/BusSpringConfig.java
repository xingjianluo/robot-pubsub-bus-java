package com.github.xingjianluo.robotpubsubbusjava;

import com.github.xingjianluo.robotpubsubbusjava.bus.BusConfig;
import com.github.xingjianluo.robotpubsubbusjava.bus.MessageBus;
import com.github.xingjianluo.robotpubsubbusjava.bus.SessionAuthenticator;
import com.github.xingjianluo.robotpubsubbusjava.bus.SessionIdentity;
import com.github.xingjianluo.robotpubsubbusjava.bus.StaticTokenAuthenticator;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;

/**
 * 消息总线 Spring 装配：从配置读取资源上限与本地令牌表，
 * 注册默认主题，并以服务端核验器驱动建会话。
 */
@Configuration
@EnableConfigurationProperties(BusProperties.class)
public class BusSpringConfig {

    @Bean
    public SessionAuthenticator sessionAuthenticator(BusProperties properties) {
        Map<String, SessionIdentity> identities = new HashMap<>();
        for (BusProperties.TokenIdentity token : properties.getTokens()) {
            identities.put(token.getToken(), new SessionIdentity(
                    token.getClientId(), token.getPublishTopics(), token.getSubscribeTopics()));
        }
        return new StaticTokenAuthenticator(identities);
    }

    @Bean
    public MessageBus messageBus(BusProperties properties, SessionAuthenticator authenticator) {
        MessageBus bus = new MessageBus(new BusConfig(
                properties.getMaxBacklogPerSubscription(),
                properties.getAckTimeoutMillis(),
                properties.getMaxPayloadBytes(),
                properties.getMaxDropsBeforeDisconnect(),
                properties.getMaxBacklogPerSession(),
                properties.getMaxPollWaitMillis()),
                Clock.systemUTC(), authenticator);
        properties.getTopics().forEach(bus::registerTopic);
        return bus;
    }
}
