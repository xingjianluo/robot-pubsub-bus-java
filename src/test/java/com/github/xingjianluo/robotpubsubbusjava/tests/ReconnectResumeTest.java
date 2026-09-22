package com.github.xingjianluo.robotpubsubbusjava.tests;

import com.github.xingjianluo.robotpubsubbusjava.bus.Delivery;
import com.github.xingjianluo.robotpubsubbusjava.bus.MessageBus;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 断线重连场景：重连后从已确认位点继续，已确认消息不重复交付，
 * 断连前未确认的消息以重投形式优先交付。
 */
class ReconnectResumeTest extends BusTestSupport {

    @Test
    void reconnectResumesFromAckedPosition() {
        MessageBus bus = newBus(100, 5_000, 3);
        String publisher = openSession(bus, "publisher");
        String subscriber = openSession(bus, "subscriber");
        bus.subscribe(subscriber, TOPIC);

        for (int i = 1; i <= 3; i++) {
            bus.publish(publisher, TOPIC, payload("cmd-" + i));
        }
        List<Delivery> first = bus.poll(subscriber, TOPIC, 10);
        logDeliveries(subscriber, first, "首次投递 seq=1..3");
        assertEquals(List.of(1L, 2L, 3L), first.stream().map(d -> d.message().seq()).toList());

        bus.ack(subscriber, TOPIC, 2);
        logAck(subscriber, bus, 2);

        bus.disconnect(subscriber);
        log.info("[断连] sessionId={} 确认位点={} seq=3 未确认", subscriber, bus.ackedPosition(subscriber, TOPIC));
        bus.publish(publisher, TOPIC, payload("cmd-4"));
        bus.publish(publisher, TOPIC, payload("cmd-5"));

        bus.reconnect(subscriber);
        log.info("[重连] sessionId={} 从确认位点 {} 续传", subscriber, bus.ackedPosition(subscriber, TOPIC));
        List<Delivery> resumed = bus.poll(subscriber, TOPIC, 10);
        logDeliveries(subscriber, resumed, "重连后续传：seq=3 重投，seq=4,5 首投");

        assertEquals(List.of(3L, 4L, 5L), resumed.stream().map(d -> d.message().seq()).toList(),
                "判定依据：已确认的 seq=1,2 不得重复交付");
        assertTrue(resumed.get(0).redelivered(), "判定依据：断连前未确认的 seq=3 必须标记为重投");
        assertFalse(resumed.get(1).redelivered());
        assertFalse(resumed.get(2).redelivered());

        bus.ack(subscriber, TOPIC, 5);
        logAck(subscriber, bus, 5);
        assertTrue(bus.poll(subscriber, TOPIC, 10).isEmpty(),
                "判定依据：全部确认后不得再有投递");
    }
}
