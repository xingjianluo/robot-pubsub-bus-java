package com.github.xingjianluo.robotpubsubbusjava.tests;

import com.github.xingjianluo.robotpubsubbusjava.bus.BusConfig;
import com.github.xingjianluo.robotpubsubbusjava.bus.Delivery;
import com.github.xingjianluo.robotpubsubbusjava.bus.MessageBus;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 并发一致性：多线程并发发布、拉取、确认交错时，主题序号与确认位点不串位；
 * 慢会话的有界等待只阻塞自己，不拖住其他会话的发布与拉取。
 */
class ConcurrencyTest extends BusTestSupport {

    @Test
    void concurrentPublishPollAckKeepsSequenceAndAckPositionConsistent() throws Exception {
        MessageBus bus = newBus(1_000, 60_000, 100);
        String publisher = openSession(bus, "publisher");
        String subscriber = openSession(bus, "subscriber");
        bus.subscribe(subscriber, TOPIC);

        int perPublisher = 250;
        int total = perPublisher * 2;
        ExecutorService pool = Executors.newFixedThreadPool(3);
        CountDownLatch start = new CountDownLatch(1);
        List<Long> received = new CopyOnWriteArrayList<>();
        try {
            Runnable publishHalf = () -> {
                await(start);
                for (int i = 0; i < perPublisher; i++) {
                    bus.publish(publisher, TOPIC, payload("cmd"));
                }
            };
            Future<?> p1 = pool.submit(publishHalf);
            Future<?> p2 = pool.submit(publishHalf);
            Future<?> consumer = pool.submit(() -> {
                await(start);
                long expected = 1L;
                while (expected <= total) {
                    for (Delivery d : bus.poll(subscriber, TOPIC, 50, 5)) {
                        long seq = d.message().seq();
                        if (seq != expected) {
                            throw new AssertionError("序号串位：期望 " + expected + " 实际 " + seq);
                        }
                        received.add(seq);
                        bus.ack(subscriber, TOPIC, seq); // 交错确认
                        expected++;
                    }
                }
            });
            start.countDown();
            p1.get(30, TimeUnit.SECONDS);
            p2.get(30, TimeUnit.SECONDS);
            consumer.get(30, TimeUnit.SECONDS);
        } finally {
            pool.shutdownNow();
        }
        assertEquals(total, received.size(), "判定依据：并发下消息不丢不重");
        assertEquals(total, bus.ackedPosition(subscriber, TOPIC),
                "判定依据：交错确认后确认位点推进到最终序号，不串位");
    }

    @Test
    void slowSessionBoundedWaitDoesNotBlockOthers() {
        MessageBus bus = new MessageBus(new BusConfig(100, 60_000, 1024, 3, 1_000, 150), clock);
        bus.registerTopic(TOPIC);
        String publisher = openSession(bus, "publisher");
        String slow = openSession(bus, "slow-consumer");
        String fast = openSession(bus, "fast-consumer");
        bus.subscribe(slow, TOPIC);
        bus.subscribe(fast, TOPIC);

        assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
            ExecutorService pool = Executors.newSingleThreadExecutor();
            try {
                // 慢会话请求等待 60 秒，但被 maxPollWaitMillis=150 截断为有界等待
                Future<?> slowPoll = pool.submit(() -> bus.poll(slow, TOPIC, 10, 60_000));
                long startNanos = System.nanoTime();
                for (int i = 0; i < 50; i++) {
                    bus.publish(publisher, TOPIC, payload("cmd-" + i));
                    bus.poll(fast, TOPIC, 10);
                }
                long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000;
                log.info("[隔离] 慢会话等待期间快会话完成 50 轮发布+拉取，耗时={}ms", elapsedMillis);
                assertTrue(elapsedMillis < 5_000, "判定依据：慢会话的等待不拖住其他会话");
                slowPoll.get(5, TimeUnit.SECONDS);
            } finally {
                pool.shutdownNow();
            }
        }, "判定依据：慢会话等待有界（被配置截断），整体在有限时间内完成");
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
