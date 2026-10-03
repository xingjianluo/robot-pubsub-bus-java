package com.github.xingjianluo.robotpubsubbusjava.tests;

import com.github.xingjianluo.robotpubsubbusjava.bus.BusConfig;
import com.github.xingjianluo.robotpubsubbusjava.bus.BusException;
import com.github.xingjianluo.robotpubsubbusjava.bus.Delivery;
import com.github.xingjianluo.robotpubsubbusjava.bus.ErrorCode;
import com.github.xingjianluo.robotpubsubbusjava.bus.MessageBus;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 断线对正在等待中的拉取的拦截：客户端发起带等待时间的拉取、当时没有可投递内容，
 * 等待期间会话被断开的，这次拉取必须被明确挡住（SESSION_DISCONNECTED），
 * 不能在等待结束后仍把消息（含重投消息）返回给已断线的会话；
 * 只有重连成功之后才能继续收。
 */
class DisconnectWaitingPollTest extends BusTestSupport {

    /** 拉取等待上限放宽到 10 秒，足以观察“等待中被断线”的行为。 */
    private MessageBus newWaitingBus() {
        MessageBus bus = new MessageBus(new BusConfig(100, 60_000, 1024, 3, 1_000, 10_000), clock);
        bus.registerTopic(TOPIC);
        return bus;
    }

    @Test
    void waitingPollIsRejectedWhenSessionDisconnectsMidWait() throws Exception {
        MessageBus bus = newWaitingBus();
        String publisher = openSession(bus, "publisher");
        String subscriber = openSession(bus, "subscriber");
        bus.subscribe(subscriber, TOPIC);

        // 先投一条并拉走但不确认：断线时它会被置为待重投，
        // 用于验证“等待结束后重投消息也不能返回给已断线会话”
        bus.publish(publisher, TOPIC, payload("cmd-1"));
        List<Delivery> first = bus.poll(subscriber, TOPIC, 10);
        assertEquals(List.of(1L), first.stream().map(d -> d.message().seq()).toList());

        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            // 当时没有可投递内容，这次拉取进入等待
            Future<List<Delivery>> waitingPoll =
                    pool.submit(() -> bus.poll(subscriber, TOPIC, 10, 5_000));
            Thread.sleep(300); // 确认拉取已在等待中

            long disconnectAt = System.nanoTime();
            bus.disconnect(subscriber);
            log.info("[断连] sessionId={} 处于等待中的拉取应被立即挡住", subscriber);

            ExecutionException failure = null;
            long elapsedMillis;
            try {
                waitingPoll.get(3, TimeUnit.SECONDS);
                elapsedMillis = (System.nanoTime() - disconnectAt) / 1_000_000;
            } catch (ExecutionException e) {
                failure = e;
                elapsedMillis = (System.nanoTime() - disconnectAt) / 1_000_000;
            }
            log.info("[拦截] 等待中的拉取在 {}ms 内被拒绝", elapsedMillis);
            assertTrue(elapsedMillis < 3_000,
                    "判定依据：断线立即唤醒等待中的拉取，不拖到等待自然结束");
            BusException cause = assertInstanceOf(BusException.class,
                    failure == null ? null : failure.getCause(),
                    "判定依据：等待中的拉取必须以 BusException 被拒绝，不能返回消息");
            assertEquals(ErrorCode.SESSION_DISCONNECTED, cause.code(),
                    "判定依据：断线生效后任何拉取（含重投）都不再返回消息");

            // 重连后才能继续收：断线前未确认的 seq=1 以重投形式交付
            bus.reconnect(subscriber);
            List<Delivery> resumed = bus.poll(subscriber, TOPIC, 10);
            logDeliveries(subscriber, resumed, "重连后重投断线前未确认的消息");
            assertEquals(List.of(1L), resumed.stream().map(d -> d.message().seq()).toList());
            assertTrue(resumed.get(0).redelivered(),
                    "判定依据：断线未丢消息，重连后重投交付（至少一次）");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void messageArrivingDuringDisconnectedWaitIsNotReturned() throws Exception {
        MessageBus bus = newWaitingBus();
        String publisher = openSession(bus, "publisher");
        String subscriber = openSession(bus, "subscriber");
        bus.subscribe(subscriber, TOPIC);

        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            Future<List<Delivery>> waitingPoll =
                    pool.submit(() -> bus.poll(subscriber, TOPIC, 10, 5_000));
            Thread.sleep(300);

            bus.disconnect(subscriber);
            // 断线期间其他会话照常发布：消息入队会唤醒等待中的拉取，
            // 但被唤醒的拉取仍必须先过断线检查，不能把这条消息带出去
            bus.publish(publisher, TOPIC, payload("cmd-1"));
            log.info("[断连期间发布] seq=1 到达，等待中的拉取被唤醒");

            ExecutionException failure = null;
            try {
                waitingPoll.get(3, TimeUnit.SECONDS);
            } catch (ExecutionException e) {
                failure = e;
            }
            BusException cause = assertInstanceOf(BusException.class,
                    failure == null ? null : failure.getCause(),
                    "判定依据：等待期间到达的消息不能返回给已断线的会话");
            assertEquals(ErrorCode.SESSION_DISCONNECTED, cause.code());

            bus.reconnect(subscriber);
            List<Delivery> resumed = bus.poll(subscriber, TOPIC, 10);
            logDeliveries(subscriber, resumed, "重连后收到断线期间积压的消息");
            assertEquals(List.of(1L), resumed.stream().map(d -> d.message().seq()).toList(),
                    "判定依据：断线期间的消息不丢失，重连后按序续传");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void waitingPollReturnsNormallyWhenNothingArrives() throws Exception {
        MessageBus bus = newWaitingBus();
        String subscriber = openSession(bus, "subscriber");
        bus.subscribe(subscriber, TOPIC);

        // 对照组：不断线、无消息时，等待自然结束并返回空（不报错、不挂死）
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            Future<List<Delivery>> waitingPoll =
                    pool.submit(() -> bus.poll(subscriber, TOPIC, 10, 300));
            List<Delivery> out = waitingPoll.get(3, TimeUnit.SECONDS);
            assertTrue(out.isEmpty(), "判定依据：未断线时等待自然结束返回空列表");
            log.info("[对照] 未断线的等待拉取自然结束，返回空");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void waitingPollDoesNotHangAfterDisconnect() throws Exception {
        MessageBus bus = newWaitingBus();
        String subscriber = openSession(bus, "subscriber");
        bus.subscribe(subscriber, TOPIC);

        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            Future<List<Delivery>> waitingPoll =
                    pool.submit(() -> bus.poll(subscriber, TOPIC, 10, 60_000));
            Thread.sleep(300);
            bus.disconnect(subscriber);
            // 即使等待时间远长于测试耐心，断线也必须立刻唤醒而不是挂到超时
            try {
                waitingPoll.get(3, TimeUnit.SECONDS);
            } catch (ExecutionException e) {
                assertInstanceOf(BusException.class, e.getCause());
                assertEquals(ErrorCode.SESSION_DISCONNECTED,
                        ((BusException) e.getCause()).code());
                log.info("[拦截] 长等待被断线立即打断");
                return;
            } catch (TimeoutException e) {
                throw new AssertionError("判定依据：断线后等待中的拉取不得挂到等待自然结束", e);
            }
            throw new AssertionError("判定依据：断线后等待中的拉取不得正常返回");
        } finally {
            pool.shutdownNow();
        }
    }
}
