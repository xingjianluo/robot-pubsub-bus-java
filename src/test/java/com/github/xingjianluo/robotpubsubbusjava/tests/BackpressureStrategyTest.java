package com.github.xingjianluo.robotpubsubbusjava.tests;

import com.github.xingjianluo.robotpubsubbusjava.bus.BackpressureStrategy;
import com.github.xingjianluo.robotpubsubbusjava.bus.BusException;
import com.github.xingjianluo.robotpubsubbusjava.bus.Delivery;
import com.github.xingjianluo.robotpubsubbusjava.bus.ErrorCode;
import com.github.xingjianluo.robotpubsubbusjava.bus.MessageBus;
import com.github.xingjianluo.robotpubsubbusjava.bus.SeqGap;
import com.github.xingjianluo.robotpubsubbusjava.bus.SessionEvent;
import com.github.xingjianluo.robotpubsubbusjava.bus.SessionEventType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 按订阅选择背压策略：
 * <ul>
 *   <li>EVICT_SUBSCRIPTION：积压满即摘除，产生 BACKLOG_GAP 与 SUBSCRIPTION_EVICTED 两类可区分事件；</li>
 *   <li>摘除只废订阅不废会话：会话仍在、其他订阅可用；</li>
 *   <li>重连后订阅重挂，从已确认位点续收，摘除期间缺失序号以缺口事件补报。</li>
 * </ul>
 */
class BackpressureStrategyTest extends BusTestSupport {

    @Test
    void evictStrategyDropsSubscriptionButKeepsSessionAndResumesAfterReconnect() {
        // 单订阅积压上限 2，会话总量 10（本例只触发订阅自身策略）
        MessageBus bus = new MessageBus(
                new com.github.xingjianluo.robotpubsubbusjava.bus.BusConfig(2, 10, 1024, 3),
                clock);
        bus.registerTopic(TOPIC);
        String publisher = openSession(bus, "publisher");
        String subscriber = openSession(bus, "evict-subscriber");
        bus.subscribe(subscriber, TOPIC, BackpressureStrategy.EVICT_SUBSCRIPTION);

        // seq=1,2 填满积压（不拉取）
        bus.publish(publisher, TOPIC, payload("cmd-1"));
        bus.publish(publisher, TOPIC, payload("cmd-2"));
        assertTrue(bus.drainEvents(subscriber).isEmpty(), "判定依据：积压未满不得有事件");

        // seq=3 触发 EVICT：作废积压 1,2 与新来的 3，事件含缺口 1..3 与摘除两类
        bus.publish(publisher, TOPIC, payload("cmd-3"));
        List<SessionEvent> events = bus.drainEvents(subscriber);
        log.info("[事件] 摘除触发事件数={}", events.size());
        long backlogGaps = events.stream().filter(e -> e.type() == SessionEventType.BACKLOG_GAP).count();
        long evicts = events.stream().filter(e -> e.type() == SessionEventType.SUBSCRIPTION_EVICTED).count();
        assertEquals(1, backlogGaps, "判定依据：丢弃要以 BACKLOG_GAP 标出缺口");
        assertEquals(1, evicts, "判定依据：摘除要以 SUBSCRIPTION_EVICTED 与丢弃区分");
        SessionEvent evictEvent = events.stream()
                .filter(e -> e.type() == SessionEventType.SUBSCRIPTION_EVICTED).findFirst().orElseThrow();
        assertEquals(List.of(new SeqGap(1L, 3L)), evictEvent.gaps(),
                "判定依据：作废序号 1..3 应合并为单个可识别缺口");

        // 摘除后拉取被显式拒绝，但会话本身还在
        BusException ex = assertThrows(BusException.class, () -> bus.poll(subscriber, TOPIC, 10));
        log.info("[拒绝] sessionId={} 错误码={} 判定依据=订阅已摘除但会话保留", subscriber, ex.code());
        assertEquals(ErrorCode.SUBSCRIPTION_EVICTED, ex.code());
        assertTrue(bus.isConnected(subscriber), "判定依据：订阅摘除不得作废会话");

        // 摘除期间发布的消息不缓冲，但序号仍被跟踪（seq=4）
        bus.publish(publisher, TOPIC, payload("cmd-4"));

        // 重连：订阅重挂，补报摘除期间缺失，随后从已确认位点（0）续收新消息
        bus.reconnect(subscriber);
        List<SessionEvent> resumeEvents = bus.drainEvents(subscriber);
        assertEquals(1, resumeEvents.size());
        assertEquals(SessionEventType.BACKLOG_GAP, resumeEvents.get(0).type());
        assertEquals(List.of(new SeqGap(4L, 4L)), resumeEvents.get(0).gaps(),
                "判定依据：摘除期间到达的 seq=4 重挂时补报为缺口");

        // seq=5 起正常投递，订阅恢复工作
        bus.publish(publisher, TOPIC, payload("cmd-5"));
        List<Delivery> resumed = bus.poll(subscriber, TOPIC, 10);
        logDeliveries(subscriber, resumed, "重挂后只收到重挂之后的新消息 seq=5");
        assertEquals(List.of(5L), resumed.stream().map(d -> d.message().seq()).toList(),
                "判定依据：被作废的 1..4 不再投递，订阅从位点续收");
        assertFalse(resumed.get(0).redelivered());

        bus.ack(subscriber, TOPIC, 5);
        assertTrue(bus.poll(subscriber, TOPIC, 10).isEmpty());
    }

    @Test
    void dropOldestStrategyEmitsOnlyGapEventsUntilThreshold() {
        MessageBus bus = new MessageBus(
                new com.github.xingjianluo.robotpubsubbusjava.bus.BusConfig(2, 10, 1024, 3),
                clock);
        bus.registerTopic(TOPIC);
        String publisher = openSession(bus, "publisher");
        String subscriber = openSession(bus, "drop-subscriber");
        bus.subscribe(subscriber, TOPIC, BackpressureStrategy.DROP_OLDEST);

        for (int i = 1; i <= 4; i++) {
            bus.publish(publisher, TOPIC, payload("cmd-" + i));
        }
        // 上限2：seq3 丢 seq1，seq4 丢 seq2 —— 尚未达到连续丢弃阈值 3，订阅仍在
        List<SessionEvent> events = bus.drainEvents(subscriber);
        assertEquals(2, events.size());
        assertTrue(events.stream().allMatch(e -> e.type() == SessionEventType.BACKLOG_GAP));
        assertEquals(List.of(new SeqGap(1L, 1L)), events.get(0).gaps());
        assertEquals(List.of(new SeqGap(2L, 2L)), events.get(1).gaps());

        List<Delivery> deliveries = bus.poll(subscriber, TOPIC, 10);
        assertEquals(List.of(3L, 4L), deliveries.stream().map(d -> d.message().seq()).toList(),
                "判定依据：丢弃最旧后保留最新两条");
    }
}
