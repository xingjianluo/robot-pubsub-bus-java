package com.github.xingjianluo.robotpubsubbusjava.tests;

import com.github.xingjianluo.robotpubsubbusjava.bus.BackpressurePolicy;
import com.github.xingjianluo.robotpubsubbusjava.bus.BusEvent;
import com.github.xingjianluo.robotpubsubbusjava.bus.BusException;
import com.github.xingjianluo.robotpubsubbusjava.bus.Delivery;
import com.github.xingjianluo.robotpubsubbusjava.bus.ErrorCode;
import com.github.xingjianluo.robotpubsubbusjava.bus.EventKind;
import com.github.xingjianluo.robotpubsubbusjava.bus.MessageBus;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 按订阅选择背压策略：EVICT_SUBSCRIPTION 一旦超限即摘除订阅；
 * 订阅被摘除后会话保留，重连后从已确认位点接着收，会话不因一次背压作废。
 */
class BackpressurePolicyTest extends BusTestSupport {

    @Test
    void evictSubscriptionPolicyEvictsImmediatelyAndSessionSurvives() {
        MessageBus bus = newBus(2, 60_000, 100); // 积压上限 2，丢弃阈值很高：DROP_OLDEST 不会摘除
        String publisher = openSession(bus, "publisher");
        String evicting = openSession(bus, "evicting-subscriber");
        String dropping = openSession(bus, "dropping-subscriber");
        bus.subscribe(evicting, TOPIC, BackpressurePolicy.EVICT_SUBSCRIPTION);
        bus.subscribe(dropping, TOPIC, BackpressurePolicy.DROP_OLDEST);

        for (int i = 1; i <= 4; i++) {
            bus.publish(publisher, TOPIC, payload("cmd-" + i));
        }

        // EVICT_SUBSCRIPTION：第一次超限即被摘除
        BusException evicted = assertThrows(BusException.class,
                () -> bus.poll(evicting, TOPIC, 10));
        log.info("[拒绝] sessionId={} 错误码={} 判定依据=EVICT_SUBSCRIPTION 首次超限即摘除",
                evicting, evicted.code());
        assertEquals(ErrorCode.SUBSCRIPTION_EVICTED, evicted.code());
        List<BusEvent> events = bus.drainEvents(evicting);
        events.forEach(e -> log.info("[事件] kind={} missing={}", e.kind(), e.missingSeqs()));
        assertTrue(events.stream().anyMatch(e -> e.kind() == EventKind.SUBSCRIPTION_EVICTED));

        // DROP_OLDEST：同样积压下不被摘除，仍能拉到最旧被丢弃后的消息
        List<Delivery> dropped = bus.poll(dropping, TOPIC, 10);
        logDeliveries(dropping, dropped, "DROP_OLDEST 保留最新两条");
        assertEquals(List.of(3L, 4L), dropped.stream().map(d -> d.message().seq()).toList(),
                "判定依据：同一会话级场景下两种策略按订阅独立生效");

        // 会话保留：确认位点仍在，重连后从已确认位点续传
        bus.disconnect(evicting);
        bus.reconnect(evicting);
        log.info("[重连] sessionId={} 确认位点={} 订阅复活", evicting, bus.ackedPosition(evicting, TOPIC));
        bus.publish(publisher, TOPIC, payload("cmd-5"));
        List<Delivery> resumed = bus.poll(evicting, TOPIC, 10);
        logDeliveries(evicting, resumed, "摘除后重连续传新消息");
        assertEquals(List.of(5L), resumed.stream().map(d -> d.message().seq()).toList(),
                "判定依据：订阅被摘除后会话不作废，重连后继续接收新消息");

        bus.ack(evicting, TOPIC, 5);
        logAck(evicting, bus, 5);
        assertEquals(5L, bus.ackedPosition(evicting, TOPIC),
                "判定依据：确认位点在摘除/重连后仍正常推进");
    }
}
