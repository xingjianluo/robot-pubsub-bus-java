package com.github.xingjianluo.robotpubsubbusjava.tests;

import com.github.xingjianluo.robotpubsubbusjava.bus.BusException;
import com.github.xingjianluo.robotpubsubbusjava.bus.Delivery;
import com.github.xingjianluo.robotpubsubbusjava.bus.DropEvent;
import com.github.xingjianluo.robotpubsubbusjava.bus.DropReason;
import com.github.xingjianluo.robotpubsubbusjava.bus.ErrorCode;
import com.github.xingjianluo.robotpubsubbusjava.bus.MessageBus;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 慢消费者场景：积压有界，超限按"丢弃最旧"策略处理并产生可区分事件；
 * 连续丢弃超限后订阅被断开；健康订阅者不受影响。
 */
class SlowConsumerTest extends BusTestSupport {

    @Test
    void slowConsumerIsBoundedAndEvictedWithoutAffectingOthers() {
        MessageBus bus = newBus(3, 60_000, 3); // 积压上限 3，连续丢弃 3 次即断开
        String publisher = openSession(bus, "publisher");
        String slow = openSession(bus, "slow-consumer");
        String healthy = openSession(bus, "healthy-consumer");
        bus.subscribe(slow, TOPIC);
        bus.subscribe(healthy, TOPIC);

        List<Delivery> healthyDeliveries = new java.util.ArrayList<>();
        for (int i = 1; i <= 6; i++) {
            bus.publish(publisher, TOPIC, payload("cmd-" + i));
            // 健康订阅者随发随取，保持消费进度；慢订阅者从不拉取
            healthyDeliveries.addAll(bus.poll(healthy, TOPIC, 10));
        }

        List<DropEvent> events = bus.drainDropEvents(slow);
        for (DropEvent e : events) {
            log.info("[丢弃事件] sessionId={} seq={} reason={} detail={}",
                    e.sessionId(), e.seq(), e.reason(), e.detail());
        }
        assertEquals(3, events.size());
        assertEquals(DropReason.BACKLOG_OVERFLOW, events.get(0).reason());
        assertEquals(1L, events.get(0).seq(), "判定依据：丢弃最旧消息 seq=1");
        assertEquals(DropReason.BACKLOG_OVERFLOW, events.get(1).reason());
        assertEquals(DropReason.SLOW_CONSUMER_EVICTED, events.get(2).reason(),
                "判定依据：连续丢弃 3 次后订阅被断开，原因与丢弃可区分");

        BusException evicted = assertThrows(BusException.class,
                () -> bus.poll(slow, TOPIC, 10));
        log.info("[拒绝] sessionId={} 错误码={} 判定依据=订阅已断开，对客户端可见", slow, evicted.code());
        assertEquals(ErrorCode.SUBSCRIPTION_EVICTED, evicted.code());

        logDeliveries(healthy, healthyDeliveries, "健康订阅者完整接收 seq=1..6");
        assertEquals(List.of(1L, 2L, 3L, 4L, 5L, 6L),
                healthyDeliveries.stream().map(d -> d.message().seq()).toList(),
                "判定依据：慢消费者被断开不得影响其他订阅者");
    }
}
