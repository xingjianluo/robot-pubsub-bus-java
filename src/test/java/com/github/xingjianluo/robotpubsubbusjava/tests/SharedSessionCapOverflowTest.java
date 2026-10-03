package com.github.xingjianluo.robotpubsubbusjava.tests;

import com.github.xingjianluo.robotpubsubbusjava.bus.BackpressurePolicy;
import com.github.xingjianluo.robotpubsubbusjava.bus.BusConfig;
import com.github.xingjianluo.robotpubsubbusjava.bus.BusEvent;
import com.github.xingjianluo.robotpubsubbusjava.bus.Delivery;
import com.github.xingjianluo.robotpubsubbusjava.bus.EventKind;
import com.github.xingjianluo.robotpubsubbusjava.bus.MessageBus;
import com.github.xingjianluo.robotpubsubbusjava.bus.SessionIdentity;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 多订阅共享会话级总上限时的超限行为：
 * 一个主题把会话总量占满、另一个主题积压为空时，继续向空主题发布
 * 必须按该订阅的背压策略正常处理（丢弃新到消息或摘除订阅），
 * 发布正常返回、不抛服务端内部错误，且不影响其他会话对同一条消息的投递。
 */
class SharedSessionCapOverflowTest extends BusTestSupport {

    private static final String TOPIC_B = "robot.telemetry";

    private MessageBus newSharedCapBus(int perSubscription, int perSession, int maxDrops) {
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
    void publishingToEmptySubscriptionWhenSessionCapFullDropsIncomingAndKeepsOthers() {
        // 单订阅上限 4，会话总量 4：主题 A 独自即可占满会话总量
        MessageBus bus = newSharedCapBus(4, 4, 100);
        String publisher = openBothTopicsSession(bus, "publisher");
        String slow = openBothTopicsSession(bus, "slow-consumer");
        String other = openBothTopicsSession(bus, "other-consumer");
        bus.subscribe(slow, TOPIC);
        bus.subscribe(slow, TOPIC_B);
        bus.subscribe(other, TOPIC_B);

        for (int i = 1; i <= 4; i++) {
            bus.publish(publisher, TOPIC, payload("cmd-" + i));
        }
        assertEquals(4, bus.totalPending(slow), "判定依据：主题 A 已占满会话总量上限");

        // 向积压为空的主题 B 发布：容量到顶属正常背压，发布必须正常返回
        long seq1 = bus.publish(publisher, TOPIC_B, payload("tm-1"));
        long seq2 = bus.publish(publisher, TOPIC_B, payload("tm-2"));
        log.info("[背压] 会话总量已满，空订阅收到 seq={}、{}，按 DROP_OLDEST 丢弃新到消息", seq1, seq2);
        assertEquals(1L, seq1);
        assertEquals(2L, seq2);
        assertEquals(0, bus.pendingCount(slow, TOPIC_B),
                "判定依据：空订阅无最旧可丢，丢弃新到消息，积压仍为空");
        assertEquals(4, bus.totalPending(slow),
                "判定依据：会话总积压全程不突破上限");

        // 缺口事件必须完整反映丢弃的序号：恰好丢 seq=1,2，不多不少
        List<BusEvent> events = bus.drainEvents(slow);
        List<Long> missing = events.stream()
                .filter(e -> e.kind() == EventKind.GAP && TOPIC_B.equals(e.topic()))
                .flatMap(e -> e.missingSeqs().stream())
                .toList();
        log.info("[缺口事件] sessionId={} 主题B缺失序号={}", slow, missing);
        assertEquals(List.of(1L, 2L), missing,
                "判定依据：缺口事件完整反映丢弃的序号，不多一条不少一条");

        // 同一条消息对其他会话的投递不被掐断：other 按序收到 seq=1,2
        List<Delivery> otherDeliveries = bus.poll(other, TOPIC_B, 10);
        logDeliveries(other, otherDeliveries, "其他会话不受慢会话背压连累");
        assertEquals(List.of(1L, 2L),
                otherDeliveries.stream().map(d -> d.message().seq()).toList(),
                "判定依据：某一会话超限不能把该消息对其他订阅者的投递整条掐断");
        assertTrue(otherDeliveries.stream().noneMatch(Delivery::redelivered));

        // 慢会话主题 A 的积压不受影响，仍可正常拉取
        List<Delivery> commands = bus.poll(slow, TOPIC, 10);
        assertEquals(List.of(1L, 2L, 3L, 4L),
                commands.stream().map(d -> d.message().seq()).toList());
    }

    @Test
    void evictPolicyOnEmptySubscriptionKeepsSessionAliveAndResumesAfterReconnect() {
        // 会话总量 2：主题 A 占满后，主题 B（EVICT_SUBSCRIPTION）收到新消息即被摘除
        MessageBus bus = newSharedCapBus(4, 2, 100);
        String publisher = openBothTopicsSession(bus, "publisher");
        String slow = openBothTopicsSession(bus, "slow-consumer");
        bus.subscribe(slow, TOPIC);
        bus.subscribe(slow, TOPIC_B, BackpressurePolicy.EVICT_SUBSCRIPTION);

        // 先让主题 B 确认到 seq=1，验证重连后从已确认位点续传
        bus.publish(publisher, TOPIC_B, payload("tm-1"));
        List<Delivery> first = bus.poll(slow, TOPIC_B, 10);
        assertEquals(List.of(1L), first.stream().map(d -> d.message().seq()).toList());
        bus.ack(slow, TOPIC_B, 1);
        log.info("[确认] sessionId={} 主题B确认位点={}", slow, bus.ackedPosition(slow, TOPIC_B));

        // 主题 A 占满会话总量（2 条）
        bus.publish(publisher, TOPIC, payload("cmd-1"));
        bus.publish(publisher, TOPIC, payload("cmd-2"));
        assertEquals(2, bus.totalPending(slow));

        // 空订阅 B 收到新消息：按 EVICT_SUBSCRIPTION 摘除订阅，发布正常返回
        long seq = bus.publish(publisher, TOPIC_B, payload("tm-2"));
        log.info("[摘除] 会话总量已满，主题B收到 seq={} 触发 EVICT_SUBSCRIPTION", seq);
        assertEquals(2L, seq);

        List<BusEvent> events = bus.drainEvents(slow);
        List<Long> missing = events.stream()
                .filter(e -> e.kind() == EventKind.GAP && TOPIC_B.equals(e.topic()))
                .flatMap(e -> e.missingSeqs().stream())
                .toList();
        assertEquals(List.of(2L), missing, "判定依据：被丢弃的 seq=2 由缺口事件完整反映");
        assertTrue(events.stream().anyMatch(e -> e.kind() == EventKind.SUBSCRIPTION_EVICTED
                        && TOPIC_B.equals(e.topic())),
                "判定依据：摘除事件与缺口事件可区分");
        log.info("[事件] 主题B缺口={}，含 SUBSCRIPTION_EVICTED", missing);

        // 订阅被摘除不能把会话作废：会话仍连接，主题 A 可正常拉取确认
        List<Delivery> commands = bus.poll(slow, TOPIC, 10);
        assertEquals(List.of(1L, 2L), commands.stream().map(d -> d.message().seq()).toList());
        bus.ack(slow, TOPIC, 2);

        // 重连后被摘除的订阅复活，从已确认位点（seq=1）之后继续收新消息
        bus.reconnect(slow);
        log.info("[重连] sessionId={} 主题B确认位点={}", slow, bus.ackedPosition(slow, TOPIC_B));
        bus.publish(publisher, TOPIC_B, payload("tm-3"));
        List<Delivery> resumed = bus.poll(slow, TOPIC_B, 10);
        logDeliveries(slow, resumed, "重连后主题B从已确认位点续传");
        assertEquals(List.of(3L), resumed.stream().map(d -> d.message().seq()).toList(),
                "判定依据：已确认的 seq=1 不重复交付，被丢弃的 seq=2 不补发，只续传新消息");
        assertFalse(resumed.get(0).redelivered());
        assertEquals(1L, bus.ackedPosition(slow, TOPIC_B),
                "判定依据：确认位点不被摘除/重连串位");
    }
}
