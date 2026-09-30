package com.github.xingjianluo.robotpubsubbusjava.tests;

import com.github.xingjianluo.robotpubsubbusjava.bus.BusConfig;
import com.github.xingjianluo.robotpubsubbusjava.bus.BusException;
import com.github.xingjianluo.robotpubsubbusjava.bus.Delivery;
import com.github.xingjianluo.robotpubsubbusjava.bus.ErrorCode;
import com.github.xingjianluo.robotpubsubbusjava.bus.MessageBus;
import com.github.xingjianluo.robotpubsubbusjava.bus.SessionIdentity;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 会话级总量上限：单会话全部订阅积压之和受 maxBacklogPerSession 约束，
 * 多主题同时积压也不能突破总量，不随订阅主题数无界增长。
 */
class SessionCapacityTest extends BusTestSupport {

    private static final String TOPIC_B = "robot.telemetry";

    private MessageBus newCappedBus(int perSubscription, int perSession, int maxDrops) {
        MessageBus bus = new MessageBus(
                new BusConfig(perSubscription, 60_000, 1024, maxDrops, perSession, 200), clock);
        bus.registerTopic(TOPIC);
        bus.registerTopic(TOPIC_B);
        return bus;
    }

    private String openBothTopicsSession(MessageBus bus, String clientId) {
        String sessionId = bus.openSession(new SessionIdentity(
                clientId, Set.of(TOPIC, TOPIC_B), Set.of(TOPIC, TOPIC_B)));
        log.info("[会话开启] sessionId={} clientId={} 授权范围=两主题", sessionId, clientId);
        return sessionId;
    }

    @Test
    void sessionTotalBacklogIsBoundedAcrossTopics() {
        MessageBus bus = newCappedBus(4, 5, 100); // 单订阅 4，会话总量 5
        String publisher = openBothTopicsSession(bus, "publisher");
        String slow = openBothTopicsSession(bus, "slow-consumer");
        bus.subscribe(slow, TOPIC);
        bus.subscribe(slow, TOPIC_B);

        for (int i = 1; i <= 4; i++) {
            bus.publish(publisher, TOPIC, payload("cmd-" + i));
        }
        assertEquals(4, bus.pendingCount(slow, TOPIC));

        // 会话总量只剩 1 个位置：第二个主题最多再积压 1 条，之后按策略丢弃最旧
        for (int i = 1; i <= 4; i++) {
            bus.publish(publisher, TOPIC_B, payload("tm-" + i));
            int total = bus.totalPending(slow);
            log.info("[容量] 第 {} 条遥测发布后会话总积压={} 上限=5", i, total);
            assertTrue(total <= 5, "判定依据：多主题同时积压不得突破会话总量上限");
        }
        assertEquals(4, bus.pendingCount(slow, TOPIC),
                "判定依据：第一主题积压不受第二主题背压影响");
        assertEquals(1, bus.pendingCount(slow, TOPIC_B),
                "判定依据：第二主题只能占用会话剩余容量");

        List<Delivery> commands = bus.poll(slow, TOPIC, 10);
        assertEquals(List.of(1L, 2L, 3L, 4L),
                commands.stream().map(d -> d.message().seq()).toList());
        List<Delivery> telemetry = bus.poll(slow, TOPIC_B, 10);
        assertEquals(List.of(4L), telemetry.stream().map(d -> d.message().seq()).toList(),
                "判定依据：会话总量挤占时按丢弃最旧处理，只保留最新一条");
    }

    @Test
    void sessionCapTriggersEvictionBySubscriptionPolicy() {
        MessageBus bus = newCappedBus(10, 2, 2); // 会话总量 2，连续丢弃 2 次摘除
        String publisher = openSession(bus, "publisher");
        String slow = openSession(bus, "slow-consumer");
        bus.subscribe(slow, TOPIC);

        for (int i = 1; i <= 5; i++) {
            bus.publish(publisher, TOPIC, payload("cmd-" + i));
        }
        BusException evicted = assertThrows(BusException.class, () -> bus.poll(slow, TOPIC, 10));
        log.info("[拒绝] 错误码={} 判定依据=会话总量上限同样触发订阅背压策略", evicted.code());
        assertEquals(ErrorCode.SUBSCRIPTION_EVICTED, evicted.code());
    }
}
