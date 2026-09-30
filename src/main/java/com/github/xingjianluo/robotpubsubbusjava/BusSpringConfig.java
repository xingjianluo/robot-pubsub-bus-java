package com.github.xingjianluo.robotpubsubbusjava;

import com.github.xingjianluo.robotpubsubbusjava.bus.BusConfig;
import com.github.xingjianluo.robotpubsubbusjava.bus.ClientDirectory;
import com.github.xingjianluo.robotpubsubbusjava.bus.InMemoryClientDirectory;
import com.github.xingjianluo.robotpubsubbusjava.bus.MessageBus;
import com.github.xingjianluo.robotpubsubbusjava.bus.RegisteredClient;
import com.github.xingjianluo.robotpubsubbusjava.web.BusProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;
import java.util.List;

/**
 * 消息总线 Spring 装配：从配置读取资源上限、主题与登记客户端，
 * 构造总线与凭据登记册。
 */
@Configuration
@EnableConfigurationProperties(BusProperties.class)
public class BusSpringConfig {

    @Bean
    public MessageBus messageBus(BusProperties props) {
        BusConfig config = new BusConfig(
                props.getMaxBacklogPerSubscription(),
                props.getMaxBacklogPerSession(),
                props.getAckTimeoutMillis(),
                props.getMaxPayloadBytes(),
                props.getMaxDropsBeforeDisconnect());
        MessageBus bus = new MessageBus(config, Clock.systemUTC());
        bus.setClientDirectory(clientDirectory(props));
        List<String> topics = props.getTopics();
        if (topics.isEmpty()) {
            List.of("robot.commands", "robot.telemetry").forEach(bus::registerTopic);
        } else {
            topics.forEach(bus::registerTopic);
        }
        return bus;
    }

    private ClientDirectory clientDirectory(BusProperties props) {
        InMemoryClientDirectory directory = new InMemoryClientDirectory();
        for (BusProperties.ClientProps client : props.getClients()) {
            directory.register(new RegisteredClient(
                    client.getToken(),
                    client.getClientId(),
                    new java.util.HashSet<>(client.getPublishTopics()),
                    new java.util.HashSet<>(client.getSubscribeTopics())));
        }
        return directory;
    }
}
