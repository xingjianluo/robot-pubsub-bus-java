package com.github.xingjianluo.robotpubsubbusjava.bus.config;

import com.github.xingjianluo.robotpubsubbusjava.bus.auth.AuthorizationService;
import com.github.xingjianluo.robotpubsubbusjava.bus.auth.ConfigAuthorizationService;
import com.github.xingjianluo.robotpubsubbusjava.bus.engine.LocalMessageBus;
import com.github.xingjianluo.robotpubsubbusjava.bus.api.MessageBus;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** 总线 Spring 装配：配置 -> 授权 -> 本地总线（单例）。 */
@Configuration
@EnableConfigurationProperties(BusProperties.class)
public class BusConfig {

    @Bean
    public AuthorizationService authorizationService(BusProperties properties) {
        return new ConfigAuthorizationService(properties);
    }

    @Bean
    public MessageBus messageBus(BusProperties properties, AuthorizationService authorizationService) {
        return new LocalMessageBus(properties, authorizationService);
    }
}
