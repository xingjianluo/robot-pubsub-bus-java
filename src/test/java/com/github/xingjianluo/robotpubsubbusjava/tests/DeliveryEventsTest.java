package com.github.xingjianluo.robotpubsubbusjava.tests;

import com.github.xingjianluo.robotpubsubbusjava.bus.BusEvent;
import com.github.xingjianluo.robotpubsubbusjava.bus.EventKind;
import com.github.xingjianluo.robotpubsubbusjava.bus.MessageBus;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 投递结果事件反馈：正常投递、背压缺口（标出缺失序号）、订阅被摘除、
 * 会话被关闭四类事件彼此可区分，客户端据此判断"收到了/丢了哪段/会话没了"。
 */
class DeliveryEventsTest extends BusTestSupport {

    @Test
    void deliveryGapEvictedAndClosedEventsAreDistinguishable() {
        MessageBus bus = newBus(3, 60_000, 2); // 积压上限 3，连续丢弃 2 次即摘除
        String publisher = openSession(bus, "publisher");
        String subscriber = openSession(bus, "subscriber");
        bus.subscribe(subscriber, TOPIC);

        // 正常投递事件
        bus.publish(publisher, TOPIC, payload("cmd-1"));
        bus.poll(subscriber, TOPIC, 10);
        List<BusEvent> delivered = bus.drainEvents(subscriber);
        delivered.forEach(e -> log.info("[事件] kind={} seq={} 判定依据=正常投递", e.kind(), e.fromSeq()));
        assertEquals(1, delivered.size());
        assertEquals(EventKind.DELIVERED, delivered.get(0).kind());
        assertEquals(1L, delivered.get(0).fromSeq());

        // 背压缺口：发 5 条，积压上限 3，连续丢弃 2 次触发摘除
        for (int i = 2; i <= 6; i++) {
            bus.publish(publisher, TOPIC, payload("cmd-" + i));
        }
        List<BusEvent> events = bus.drainEvents(subscriber);
        events.forEach(e -> log.info("[事件] kind={} fromSeq={} toSeq={} missing={} 判定依据={}",
                e.kind(), e.fromSeq(), e.toSeq(), e.missingSeqs(), e.detail()));

        List<BusEvent> gaps = events.stream().filter(e -> e.kind() == EventKind.GAP).toList();
        List<BusEvent> evicted = events.stream()
                .filter(e -> e.kind() == EventKind.SUBSCRIPTION_EVICTED).toList();
        assertEquals(1, gaps.size(), "判定依据：连续丢弃的序号合并为一段可识别缺口");
        assertEquals(List.of(2L, 3L, 4L, 5L, 6L), gaps.get(0).missingSeqs(),
                "判定依据：缺口完整标出缺失序号（丢弃最旧 2,3 + 摘除时丢弃积压 4,5 与新消息 6）");
        assertEquals(1, evicted.size(), "判定依据：订阅被摘除与缺口是不同事件类别");

        // 会话关闭事件
        bus.closeSession(subscriber);
        List<BusEvent> closed = bus.drainEvents(subscriber);
        closed.forEach(e -> log.info("[事件] kind={} 判定依据=会话被关闭", e.kind()));
        assertEquals(1, closed.size());
        assertEquals(EventKind.SESSION_CLOSED, closed.get(0).kind());

        assertTrue(bus.drainEvents(subscriber).isEmpty(), "判定依据：事件读取后即清空");
    }
}
