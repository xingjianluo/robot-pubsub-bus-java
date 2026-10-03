package com.github.xingjianluo.robotpubsubbusjava.tests;

import com.github.xingjianluo.robotpubsubbusjava.bus.BusConfig;
import com.github.xingjianluo.robotpubsubbusjava.bus.BusEvent;
import com.github.xingjianluo.robotpubsubbusjava.bus.Delivery;
import com.github.xingjianluo.robotpubsubbusjava.bus.EventKind;
import com.github.xingjianluo.robotpubsubbusjava.bus.MessageBus;
import com.github.xingjianluo.robotpubsubbusjava.bus.SessionIdentity;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 并发一致性：多线程并发发布、拉取、确认交错时，主题序号与确认位点不串位；
 * 慢会话的有界等待只阻塞自己，不拖住其他会话的发布与拉取。
 */
class ConcurrencyTest extends BusTestSupport {

    private static final String TOPIC_B = "robot.telemetry";

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
    void concurrentMultiTopicSharedSessionCapStaysBoundedAndConsistent() throws Exception {
        // 单订阅上限 8、会话总量 10、丢弃阈值调高（本用例只走 DROP_OLDEST，不触发摘除）
        MessageBus bus = new MessageBus(new BusConfig(8, 60_000, 1024, 1_000_000, 10, 20), clock);
        bus.registerTopic(TOPIC);
        bus.registerTopic(TOPIC_B);
        Set<String> both = Set.of(TOPIC, TOPIC_B);
        String publisherA = bus.openSession(new SessionIdentity("pub-a", both, both));
        String publisherB = bus.openSession(new SessionIdentity("pub-b", both, both));
        String slow = bus.openSession(new SessionIdentity("slow", both, both));
        bus.subscribe(slow, TOPIC);
        bus.subscribe(slow, TOPIC_B);

        int perTopic = 200;
        ExecutorService pool = Executors.newFixedThreadPool(3);
        CountDownLatch start = new CountDownLatch(1);
        Map<String, List<Long>> received = new ConcurrentHashMap<>();
        Map<String, Set<Long>> gapped = new ConcurrentHashMap<>();
        received.put(TOPIC, new CopyOnWriteArrayList<>());
        received.put(TOPIC_B, new CopyOnWriteArrayList<>());
        gapped.put(TOPIC, ConcurrentHashMap.newKeySet());
        gapped.put(TOPIC_B, ConcurrentHashMap.newKeySet());
        AtomicInteger maxTotalPending = new AtomicInteger();
        try {
            Runnable publishLoop = new Runnable() {
                @Override
                public void run() {
                    await(start);
                }
            };
            Future<?> p1 = pool.submit(() -> {
                publishLoop.run();
                for (int i = 0; i < perTopic; i++) {
                    bus.publish(publisherA, TOPIC, payload("cmd-" + i));
                }
            });
            Future<?> p2 = pool.submit(() -> {
                publishLoop.run();
                for (int i = 0; i < perTopic; i++) {
                    bus.publish(publisherB, TOPIC_B, payload("tm-" + i));
                }
            });
            Future<?> consumer = pool.submit(() -> {
                await(start);
                long[] lastDelivered = {0L, 0L};
                String[] topics = {TOPIC, TOPIC_B};
                Set<Long> accountedA = gapped.get(TOPIC);
                Set<Long> accountedB = gapped.get(TOPIC_B);
                while (received.get(TOPIC).size() + accountedA.size() < perTopic
                        || received.get(TOPIC_B).size() + accountedB.size() < perTopic) {
                    for (int t = 0; t < 2; t++) {
                        for (Delivery d : bus.poll(slow, topics[t], 4, 2)) {
                            long seq = d.message().seq();
                            if (seq <= lastDelivered[t]) {
                                throw new AssertionError("主题 " + topics[t]
                                        + " 序号串位：上条 " + lastDelivered[t] + " 本条 " + seq);
                            }
                            lastDelivered[t] = seq;
                            received.get(topics[t]).add(seq);
                            bus.ack(slow, topics[t], seq); // 交错确认
                        }
                    }
                    for (BusEvent event : bus.drainEvents(slow)) {
                        if (event.kind() == EventKind.GAP) {
                            gapped.get(event.topic()).addAll(event.missingSeqs());
                        }
                    }
                    maxTotalPending.accumulateAndGet(bus.totalPending(slow), Math::max);
                }
            });
            start.countDown();
            p1.get(60, TimeUnit.SECONDS);
            p2.get(60, TimeUnit.SECONDS);
            consumer.get(60, TimeUnit.SECONDS);
        } finally {
            pool.shutdownNow();
        }

        log.info("[并发] 两主题各 {} 条，会话总积压峰值={}（上限 10）", perTopic, maxTotalPending.get());
        assertTrue(maxTotalPending.get() <= 10,
                "判定依据：并发发布/拉取/确认交错下会话总积压全程不突破上限");
        for (String topic : List.of(TOPIC, TOPIC_B)) {
            Set<Long> delivered = new HashSet<>(received.get(topic));
            Set<Long> missing = gapped.get(topic);
            assertEquals(received.get(topic).size(), delivered.size(),
                    "判定依据：主题 " + topic + " 投递无重复");
            assertTrue(Collections.disjoint(delivered, missing),
                    "判定依据：主题 " + topic + " 已投递序号与缺口序号不重叠");
            Set<Long> union = new HashSet<>(delivered);
            union.addAll(missing);
            assertEquals(perTopic, union.size(),
                    "判定依据：主题 " + topic + " 每条序号要么被投递要么在缺口中，不多不少");
            long lastDelivered = received.get(topic).get(received.get(topic).size() - 1);
            assertEquals(lastDelivered, bus.ackedPosition(slow, topic),
                    "判定依据：交错确认后确认位点与最后一条投递一致，不串位");
        }
    }

    @Test
    void slowSessionBoundedWaitDoesNotBlockOthers() {        MessageBus bus = new MessageBus(new BusConfig(100, 60_000, 1024, 3, 1_000, 150), clock);
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
