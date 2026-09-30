package com.github.xingjianluo.robotpubsubbusjava.tests;

import com.github.xingjianluo.robotpubsubbusjava.bus.BusException;
import com.github.xingjianluo.robotpubsubbusjava.bus.Delivery;
import com.github.xingjianluo.robotpubsubbusjava.bus.ErrorCode;
import com.github.xingjianluo.robotpubsubbusjava.bus.MessageBus;
import com.github.xingjianluo.robotpubsubbusjava.bus.SessionEvent;
import com.github.xingjianluo.robotpubsubbusjava.bus.SessionEventType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 断线与关闭语义：
 * <ul>
 *   <li>断线后发布、拉取、订阅被明确挡住（SESSION_DISCONNECTED），不存在“断线还能拉到消息”；</li>
 *   <li>断线期间消息照常为该订阅缓冲，重连后从确认位点续传；</li>
 *   <li>关闭是永久的：事件为 SESSION_CLOSED，重连以 SESSION_CLOSED 拒绝；</li>
 *   <li>事件类型彼此区分。</li>
 * </ul>
 */
class DisconnectStateTest extends BusTestSupport {

    @Test
    void disconnectedSessionIsBlockedFromPublishAndPollUntilReconnect() {
        MessageBus bus = newBus(100, 60_000, 3);
        String publisher = openSession(bus, "publisher");
        String subscriber = openSession(bus, "subscriber");
        bus.subscribe(subscriber, TOPIC);

        bus.publish(publisher, TOPIC, payload("cmd-1"));
        assertEquals(1, bus.poll(subscriber, TOPIC, 10).size());
        bus.ack(subscriber, TOPIC, 1);

        bus.disconnect(subscriber);
        log.info("[断线] sessionId={}", subscriber);
        assertFalse(bus.isConnected(subscriber));

        // 断线后：订阅者自己的拉取被明确挡住，不能再拉到消息
        BusException pollEx = assertThrows(BusException.class,
                () -> bus.poll(subscriber, TOPIC, 10));
        log.info("[拦截] 拉取错误码={}", pollEx.code());
        assertEquals(ErrorCode.SESSION_DISCONNECTED, pollEx.code());

        // 断线后：断线会话的发布也被挡住
        BusException pubEx = assertThrows(BusException.class,
                () -> bus.publish(subscriber, TOPIC, payload("x")));
        assertEquals(ErrorCode.SESSION_DISCONNECTED, pubEx.code());

        // 断线后：订阅操作也被挡住（对已授权主题重复订阅，先过授权再被连接检查拦下）
        BusException subEx = assertThrows(BusException.class,
                () -> bus.subscribe(subscriber, TOPIC));
        assertEquals(ErrorCode.SESSION_DISCONNECTED, subEx.code());

        // 断线期间别的发布者仍可发布，消息为断线订阅者缓冲
        bus.publish(publisher, TOPIC, payload("cmd-2"));
        bus.publish(publisher, TOPIC, payload("cmd-3"));

        // 重连后续传
        bus.reconnect(subscriber);
        assertTrue(bus.isConnected(subscriber));
        List<Delivery> resumed = bus.poll(subscriber, TOPIC, 10);
        logDeliveries(subscriber, resumed, "重连后续传 seq=2,3");
        assertEquals(List.of(2L, 3L), resumed.stream().map(d -> d.message().seq()).toList(),
                "判定依据：断线期间缓冲的消息重连后可收，已确认的 seq=1 不重复");
    }

    @Test
    void closedSessionEmitsEventAndCannotReconnect() {
        MessageBus bus = newBus(100, 60_000, 3);
        String s = openSession(bus, "doomed");
        bus.closeSession(s);
        log.info("[关闭] sessionId={}", s);

        List<SessionEvent> events = bus.drainEvents(s);
        assertEquals(1, events.size());
        assertEquals(SessionEventType.SESSION_CLOSED, events.get(0).type(),
                "判定依据：会话关闭事件必须与缺口/摘除区分");

        assertEquals(ErrorCode.SESSION_CLOSED,
                assertThrows(BusException.class, () -> bus.reconnect(s)).code(),
                "判定依据：关闭不可重连");
        assertEquals(ErrorCode.SESSION_CLOSED,
                assertThrows(BusException.class, () -> bus.poll(s, TOPIC, 1)).code());
        assertEquals(ErrorCode.SESSION_CLOSED,
                assertThrows(BusException.class, () -> bus.publish(s, TOPIC, payload("x"))).code());
    }

    @Test
    void unknownAndClosedSessionsAreDistinguishable() {
        MessageBus bus = newBus(100, 60_000, 3);
        String s = openSession(bus, "temp");
        bus.closeSession(s);
        assertEquals(ErrorCode.SESSION_NOT_FOUND,
                assertThrows(BusException.class, () -> bus.poll("nope", TOPIC, 1)).code());
        assertEquals(ErrorCode.SESSION_CLOSED,
                assertThrows(BusException.class, () -> bus.poll(s, TOPIC, 1)).code());
    }
}
