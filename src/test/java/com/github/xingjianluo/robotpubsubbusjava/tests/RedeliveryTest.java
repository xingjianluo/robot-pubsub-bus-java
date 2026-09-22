package com.github.xingjianluo.robotpubsubbusjava.tests;

import com.github.xingjianluo.robotpubsubbusjava.bus.Delivery;
import com.github.xingjianluo.robotpubsubbusjava.bus.MessageBus;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 重复投递场景：确认超时后未确认消息必须重投，redelivered 标志与
 * 投递次数可区分首次投递与重投；确认后不再重投。
 */
class RedeliveryTest extends BusTestSupport {

    @Test
    void unackedMessageIsRedeliveredAfterAckTimeout() {
        MessageBus bus = newBus(100, 1_000, 3);
        String publisher = openSession(bus, "publisher");
        String subscriber = openSession(bus, "subscriber");
        bus.subscribe(subscriber, TOPIC);

        bus.publish(publisher, TOPIC, payload("cmd-1"));
        List<Delivery> first = bus.poll(subscriber, TOPIC, 10);
        logDeliveries(subscriber, first, "首次投递，不确认");
        assertEquals(1, first.size());
        assertFalse(first.get(0).redelivered());
        assertEquals(1, first.get(0).attempt());

        clock.advance(500);
        assertTrue(bus.poll(subscriber, TOPIC, 10).isEmpty(),
                "判定依据：确认超时未到，不得提前重投");

        clock.advance(600); // 累计 1100ms > ackTimeout 1000ms
        List<Delivery> redelivered = bus.poll(subscriber, TOPIC, 10);
        logDeliveries(subscriber, redelivered, "确认超时后重投");
        assertEquals(1, redelivered.size());
        assertEquals(1L, redelivered.get(0).message().seq());
        assertTrue(redelivered.get(0).redelivered(), "判定依据：超时重投必须标记 redelivered=true");
        assertEquals(2, redelivered.get(0).attempt(), "判定依据：投递次数应递增");

        bus.ack(subscriber, TOPIC, 1);
        logAck(subscriber, bus, 1);
        clock.advance(10_000);
        assertTrue(bus.poll(subscriber, TOPIC, 10).isEmpty(),
                "判定依据：已确认消息即使超时也不再重投");
    }
}
