package com.github.xingjianluo.robotpubsubbusjava.tests;

import com.github.xingjianluo.robotpubsubbusjava.bus.BusConfig;
import com.github.xingjianluo.robotpubsubbusjava.bus.BusException;
import com.github.xingjianluo.robotpubsubbusjava.bus.Delivery;
import com.github.xingjianluo.robotpubsubbusjava.bus.ErrorCode;
import com.github.xingjianluo.robotpubsubbusjava.bus.MessageBus;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 断线必须真正堵死正在等待的拉取：
 * 客户端发起带等待时间的拉取、当时没有可投递内容，等待期间会话被断开，
 * 当次拉取必须被立即唤醒并明确挡住（SESSION_DISCONNECTED），
 * 不得在等待结束后把消息（含重投）返回给已断线的会话；只有重连成功后才能继续收。
 */
class DisruptWaitingPollTest extends BusTestSupport {

    @Test
    void waitingPollIsInterruptedByDisconnectAndReturnsNoMessage() throws Exception {
        // 单次拉取允许等待 5 秒，便于证明“立即被唤醒”而不是等超时
        MessageBus bus = new MessageBus(new BusConfig(100, 60_000, 1024, 3, 1_000, 5_000), clock);
        bus.registerTopic(TOPIC);
        String publisher = openSession(bus, "publisher");
        String subscriber = openSession(bus, "subscriber");
        bus.subscribe(subscriber, TOPIC);

        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            Future<List<Delivery>> waiting = pool.submit(
                    () -> bus.poll(subscriber, TOPIC, 10, 5_000));
            Thread.sleep(300); // 确保拉取线程已进入等待
            assertFalse(waiting.isDone(), "前置条件：拉取确实在等待中");

            long start = System.nanoTime();
            bus.disconnect(subscriber);
            log.info("[断连] 等待中的拉取应被立即唤醒并挡住 sessionId={}", subscriber);

            // 断线后补发一条：等待中的拉取绝不能拿到它
            long seq = bus.publish(publisher, TOPIC, payload("cmd-1"));
            log.info("[发布] 断线后发布 seq={}，等待中的拉取不得收到", seq);

            Execution outcome = captureFailure(waiting);
            long elapsedMillis = (System.nanoTime() - start) / 1_000_000;
            log.info("[等待拉取结果] 错误码={} 唤醒耗时={}ms 判定依据=无需等满5秒即被断线挡住",
                    outcome.errorCode, elapsedMillis);
            assertEquals(ErrorCode.SESSION_DISCONNECTED, outcome.errorCode);
            assertTrue(elapsedMillis < 2_000,
                    "判定依据：断线必须立即唤醒等待中的拉取，而不是等它超时后再失败");
            assertTrue(outcome.deliveries.isEmpty(),
                    "判定依据：断线后的等待拉取不得返回任何消息");

            // 未重连前再次拉取（即便已有可投递内容）也必须被挡住
            BusException stillBlocked = assertThrows(BusException.class,
                    () -> bus.poll(subscriber, TOPIC, 10, 10));
            assertEquals(ErrorCode.SESSION_DISCONNECTED, stillBlocked.code());

            // 只有重连成功后才能继续收
            bus.reconnect(subscriber);
            List<Delivery> after = bus.poll(subscriber, TOPIC, 10);
            logDeliveries(subscriber, after, "重连后才收到断线期间积压的消息");
            assertEquals(List.of(1L), after.stream().map(d -> d.message().seq()).toList());
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void waitingPollCannotReceiveRedeliveryAfterDisconnect() throws Exception {
        MessageBus bus = new MessageBus(new BusConfig(100, 60_000, 1024, 3, 1_000, 5_000), clock);
        bus.registerTopic(TOPIC);
        String publisher = openSession(bus, "publisher");
        String subscriber = openSession(bus, "subscriber");
        bus.subscribe(subscriber, TOPIC);

        // 先投递 seq=1 但不确认；之后客户端再发起一次会等待的拉取
        bus.publish(publisher, TOPIC, payload("cmd-1"));
        List<Delivery> first = bus.poll(subscriber, TOPIC, 10);
        assertEquals(List.of(1L), first.stream().map(d -> d.message().seq()).toList());
        // seq=1 尚未超时重投，拉取进入等待
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            Future<List<Delivery>> waiting = pool.submit(
                    () -> bus.poll(subscriber, TOPIC, 10, 5_000));
            Thread.sleep(300);

            // 断线把未确认的 seq=1 置为立即重投；等待中的拉取也不得把它拿回来
            bus.disconnect(subscriber);
            log.info("[断连] 未确认 seq=1 置为重投，等待中的拉取仍必须被挡住");
            Execution outcome = captureFailure(waiting);
            log.info("[等待拉取结果] 错误码={} 投递数={} 判定依据=重投也不能越过断线",
                    outcome.errorCode, outcome.deliveries.size());
            assertEquals(ErrorCode.SESSION_DISCONNECTED, outcome.errorCode);
            assertTrue(outcome.deliveries.isEmpty(),
                    "判定依据：断线生效后任何拉取都不返回消息，重投也不行");

            bus.reconnect(subscriber);
            List<Delivery> resumed = bus.poll(subscriber, TOPIC, 10);
            logDeliveries(subscriber, resumed, "重连后未确认消息以重投形式返回");
            assertEquals(List.of(1L), resumed.stream().map(d -> d.message().seq()).toList());
            assertTrue(resumed.get(0).redelivered(),
                    "判定依据：重连后收到的是重投消息，首投/重投仍可区分");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void repeatedDisconnectDuringWaitsIsAlwaysBlockedUntilReconnect() throws Exception {
        MessageBus bus = new MessageBus(new BusConfig(100, 60_000, 1024, 3, 1_000, 2_000), clock);
        bus.registerTopic(TOPIC);
        String publisher = openSession(bus, "publisher");
        String subscriber = openSession(bus, "subscriber");
        bus.subscribe(subscriber, TOPIC);

        // 连续多轮：等待 -> 断线 -> 挡住 -> 重连 -> 再等待，每轮都必须一致
        for (int round = 1; round <= 3; round++) {
            ExecutorService pool = Executors.newSingleThreadExecutor();
            try {
                Future<List<Delivery>> waiting = pool.submit(
                        () -> bus.poll(subscriber, TOPIC, 10, 2_000));
                Thread.sleep(150);
                bus.disconnect(subscriber);
                Execution outcome = captureFailure(waiting);
                log.info("[第{}轮] 等待中断线 -> 错误码={}", round, outcome.errorCode);
                assertEquals(ErrorCode.SESSION_DISCONNECTED, outcome.errorCode);
                assertTrue(outcome.deliveries.isEmpty());
                bus.reconnect(subscriber);
                long seq = bus.publish(publisher, TOPIC, payload("cmd-" + round));
                List<Delivery> got = bus.poll(subscriber, TOPIC, 10);
                assertEquals(List.of(seq), got.stream().map(d -> d.message().seq()).toList(),
                        "判定依据：第" + round + "轮重连后按序收到当轮消息");
                bus.ack(subscriber, TOPIC, seq);
            } finally {
                pool.shutdownNow();
            }
        }
    }

    private record Execution(ErrorCode errorCode, List<Delivery> deliveries) {
    }

    private static Execution captureFailure(Future<List<Delivery>> future) throws Exception {
        AtomicReference<List<Delivery>> deliveries = new AtomicReference<>(List.of());
        try {
            deliveries.set(future.get(10, TimeUnit.SECONDS));
            return new Execution(null, deliveries.get());
        } catch (java.util.concurrent.ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof BusException busException) {
                return new Execution(busException.code(), List.of());
            }
            throw e;
        }
    }
}
