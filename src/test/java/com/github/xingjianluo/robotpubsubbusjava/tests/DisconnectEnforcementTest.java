package com.github.xingjianluo.robotpubsubbusjava.tests;

import com.github.xingjianluo.robotpubsubbusjava.bus.BusException;
import com.github.xingjianluo.robotpubsubbusjava.bus.Delivery;
import com.github.xingjianluo.robotpubsubbusjava.bus.ErrorCode;
import com.github.xingjianluo.robotpubsubbusjava.bus.MessageBus;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 断线必须真正生效：断线后该会话的发布与拉取被明确挡住（SESSION_DISCONNECTED），
 * 只有重连回来才能继续，不能出现断线了还能拉到消息的情况。
 */
class DisconnectEnforcementTest extends BusTestSupport {

    @Test
    void disconnectedSessionCannotPublishOrPollUntilReconnect() {
        MessageBus bus = newBus(100, 5_000, 3);
        String publisher = openSession(bus, "publisher");
        String subscriber = openSession(bus, "subscriber");
        bus.subscribe(subscriber, TOPIC);

        bus.publish(publisher, TOPIC, payload("cmd-1"));

        bus.disconnect(subscriber);
        log.info("[断连] sessionId={}", subscriber);

        BusException pollBlocked = assertThrows(BusException.class,
                () -> bus.poll(subscriber, TOPIC, 10));
        log.info("[拒绝] sessionId={} 错误码={} 判定依据=断线会话的拉取被明确挡住",
                subscriber, pollBlocked.code());
        assertEquals(ErrorCode.SESSION_DISCONNECTED, pollBlocked.code());

        bus.disconnect(publisher);
        BusException publishBlocked = assertThrows(BusException.class,
                () -> bus.publish(publisher, TOPIC, payload("cmd-2")));
        log.info("[拒绝] sessionId={} 错误码={} 判定依据=断线会话的发布被明确挡住",
                publisher, publishBlocked.code());
        assertEquals(ErrorCode.SESSION_DISCONNECTED, publishBlocked.code());

        BusException ackBlocked = assertThrows(BusException.class,
                () -> bus.ack(subscriber, TOPIC, 1));
        assertEquals(ErrorCode.SESSION_DISCONNECTED, ackBlocked.code());

        bus.reconnect(publisher);
        bus.reconnect(subscriber);
        log.info("[重连] sessionId={} 与 {} 恢复", publisher, subscriber);
        bus.publish(publisher, TOPIC, payload("cmd-2"));

        List<Delivery> deliveries = bus.poll(subscriber, TOPIC, 10);
        logDeliveries(subscriber, deliveries, "重连后拉到断线期间积压的消息");
        assertEquals(List.of(1L, 2L), deliveries.stream().map(d -> d.message().seq()).toList(),
                "判定依据：断线期间的消息不丢失，重连后按序续传");
    }
}
