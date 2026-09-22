package com.github.xingjianluo.robotpubsubbusjava;

import com.github.xingjianluo.robotpubsubbusjava.bus.BusConfig;
import com.github.xingjianluo.robotpubsubbusjava.bus.MessageBus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;
import java.util.List;

/**
 * 消息总线 Spring 装配：从配置读取资源上限，注册机器人指令与遥测默认主题。
 */
@Configuration
public class BusSpringConfig {

    @Bean
    public MessageBus messageBus(
            @Value("${bus.max-backlog-per-subscription:100}") int maxBacklog,
            @Value("${bus.ack-timeout-millis:5000}") long ackTimeoutMillis,
            @Value("${bus.max-payload-bytes:65536}") int maxPayloadBytes,
            @Value("${bus.max-drops-before-disconnect:3}") int maxDrops,
            @Value("${bus.topics:robot.commands,robot.telemetry}") List<String> topics) {
        MessageBus bus = new MessageBus(
                new BusConfig(maxBacklog, ackTimeoutMillis, maxPayloadBytes, maxDrops),
                Clock.systemUTC());
        topics.forEach(bus::registerTopic);
        return bus;
    }
}
