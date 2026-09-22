package com.github.xingjianluo.robotpubsubbusjava.tests;

import com.github.xingjianluo.robotpubsubbusjava.bus.auth.ConfigAuthorizationService;
import com.github.xingjianluo.robotpubsubbusjava.bus.config.BusProperties;
import com.github.xingjianluo.robotpubsubbusjava.bus.engine.LocalMessageBus;
import com.github.xingjianluo.robotpubsubbusjava.bus.model.BackpressurePolicy;
import java.util.List;

/**
 * 测试总线工厂：纯内存、无外部依赖，可反复构造与关闭。
 * 身份：
 *   commander（令牌 tok-commander）：发布 robot.cmd，订阅 robot.telemetry
 *   robot-01  （令牌 tok-robot）    ：发布 robot.telemetry，订阅 robot.cmd
 *   observer  （令牌 tok-observer） ：仅发布/订阅 robot.telemetry（越权用例）
 */
final class TestBuses {

    static final String TOK_COMMANDER = "tok-commander";
    static final String TOK_ROBOT = "tok-robot";
    static final String TOK_OBSERVER = "tok-observer";

    static final String TOPIC_CMD = "robot.cmd";
    static final String TOPIC_TELEMETRY = "robot.telemetry";

    private TestBuses() {
    }

    static BusProperties defaultProps() {
        BusProperties props = new BusProperties();
        props.setAckTimeoutMillis(300);
        props.setReaperIntervalMillis(20);
        addPrincipal(props, TOK_COMMANDER, "commander",
                List.of(TOPIC_CMD, "robot.pressure"), List.of(TOPIC_TELEMETRY));
        addPrincipal(props, TOK_ROBOT, "robot-01",
                List.of(TOPIC_TELEMETRY), List.of(TOPIC_CMD, "robot.pressure"));
        addPrincipal(props, TOK_OBSERVER, "observer",
                List.of(TOPIC_TELEMETRY), List.of(TOPIC_TELEMETRY));
        addTopic(props, TOPIC_CMD, BackpressurePolicy.REJECT, 4, 64, 4096);
        addTopic(props, TOPIC_TELEMETRY, BackpressurePolicy.DROP_OLDEST, 8, 128, 4096);
        return props;
    }

    static LocalMessageBus newBus() {
        return newBus(defaultProps());
    }

    static LocalMessageBus newBus(BusProperties props) {
        return new LocalMessageBus(props, new ConfigAuthorizationService(props));
    }

    static LocalMessageBus newBusWithTopic(String topic, BackpressurePolicy policy,
                                           int maxInflight, int retention) {
        BusProperties props = new BusProperties();
        props.setAckTimeoutMillis(300);
        props.setReaperIntervalMillis(20);
        addPrincipal(props, TOK_COMMANDER, "commander",
                List.of(TOPIC_CMD, "*"), List.of(TOPIC_TELEMETRY, "*"));
        addPrincipal(props, TOK_ROBOT, "robot-01",
                List.of(TOPIC_TELEMETRY, "*"), List.of(TOPIC_CMD, "*"));
        addTopic(props, topic, policy, maxInflight, retention, 4096);
        return newBus(props);
    }

    static void addPrincipal(BusProperties props, String token, String name,
                             List<String> publish, List<String> subscribe) {
        BusProperties.PrincipalProps p = new BusProperties.PrincipalProps();
        p.setName(name);
        p.setPublish(publish);
        p.setSubscribe(subscribe);
        props.getPrincipals().put(token, p);
    }

    static void addTopic(BusProperties props, String name, BackpressurePolicy policy,
                         int maxInflight, int retention, int maxPayload) {
        BusProperties.TopicProps t = new BusProperties.TopicProps();
        t.setPolicy(policy.name());
        t.setMaxInflight(maxInflight);
        t.setRetentionSize(retention);
        t.setMaxPayloadBytes(maxPayload);
        props.getTopics().put(name, t);
    }

    static byte[] payload(String text) {
        return text.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
}
