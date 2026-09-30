package com.github.xingjianluo.robotpubsubbusjava.tests;

import com.github.xingjianluo.robotpubsubbusjava.bus.BusConfig;
import com.github.xingjianluo.robotpubsubbusjava.bus.Delivery;
import com.github.xingjianluo.robotpubsubbusjava.bus.MessageBus;
import com.github.xingjianluo.robotpubsubbusjava.bus.SessionEvent;
import com.github.xingjianluo.robotpubsubbusjava.bus.SessionEventType;
import com.github.xingjianluo.robotpubsubbusjava.bus.SessionIdentity;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 会话级资源上限与隔离：
 * <ul>
 *   <li>单会话跨订阅缓冲有总上限，多主题同时积压也不突破；</li>
 *   <li>一个慢会话的有上限等待不拖住其他会话的发布与拉取；</li>
 *   <li>并发发布/拉取/确认交错时，序号与确认位点不串位。</li>
 * </ul>
 */
class SessionCapacityTest extends BusTestSupport {

    private static final String TOPIC_A = "robot.commands";
    private static final String TOPIC_B = "robot.telemetry";

    private MessageBus newBus(int perSub, int perSession) {
        MessageBus bus = new MessageBus(new BusConfig(perSub, perSession, 60_000L, 1024, 100), clock);
        bus.registerTopic(TOPIC_A);
        bus.registerTopic(TOPIC_B);
        return bus;
    }

    private String openAllTopics(MessageBus bus, String clientId) {
        String id = bus.openSession(new SessionIdentity(
                clientId, Set.of(TOPIC_A, TOPIC_B), Set.of(TOPIC_A, TOPIC_B)));
        log.info("[会话开启] sessionId={} clientId={}", id, clientId);
        return id;
    }

    @Test
    void sessionTotalBufferIsBoundedAcrossTopics() {
        // 单订阅上限各 10，但会话总量只有 6
        MessageBus bus = newBus(10, 6);
        String publisher = openAllTopics(bus, "publisher");
        String slow = openAllTopics(bus, "slow");
        bus.subscribe(slow, TOPIC_A);
        bus.subscribe(slow, TOPIC_B);

        // 两个主题交替发布共 12 条，慢会话从不拉取
        for (int i = 1; i <= 6; i++) {
            bus.publish(publisher, TOPIC_A, payload("a-" + i));
            bus.publish(publisher, TOPIC_B, payload("b-" + i));
        }
        List<SessionEvent> events = bus.drainEvents(slow);
        long droppedSeqs = events.stream()
                .filter(e -> e.type() == SessionEventType.BACKLOG_GAP)
                .flatMap(e -> e.gaps().stream())
                .mapToLong(g -> g.toSeq() - g.fromSeq() + 1L)
                .sum();
        log.info("[容量] 两主题合计 12 条，会话总上限 6，缺口覆盖序号数={}", droppedSeqs);
        assertEquals(6L, droppedSeqs,
                "判定依据：会话总缓冲不得突破 6，恰好 6 条被丢弃且序号可识别");

        int remainingA = bus.poll(slow, TOPIC_A, 100).size();
        int remainingB = bus.poll(slow, TOPIC_B, 100).size();
        assertTrue(remainingA + remainingB <= 6,
                "判定依据：多主题同时积压时缓冲合计 " + (remainingA + remainingB) + " 不得超过总上限 6");
    }

    @Test
    void slowSessionDoesNotBlockOthers() throws Exception {
        MessageBus bus = newBus(100, 400);
        String publisher = openAllTopics(bus, "publisher");
        String slow = openAllTopics(bus, "slow");
        String fast = openAllTopics(bus, "fast");
        bus.subscribe(slow, TOPIC_A);
        bus.subscribe(fast, TOPIC_A);

        CountDownLatch started = new CountDownLatch(1);
        // 慢会话在“暂无消息”时长轮询等待（有上限），不应阻塞其他会话
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            pool.submit(() -> {
                started.countDown();
                bus.poll(slow, TOPIC_A, 10, 300L, null);
            });
            started.await();
            Thread.sleep(50); // 确保慢会话已进入等待

            long seq = bus.publish(publisher, TOPIC_A, payload("go"));
            long t0 = System.nanoTime();
            List<Delivery> fastGot = bus.poll(fast, TOPIC_A, 10);
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
            log.info("[隔离] 慢会话长轮询期间，快会话拉取耗时={}ms seq={}", elapsedMillis, seq);
            assertEquals(List.of(1L), fastGot.stream().map(d -> d.message().seq()).toList());
            assertTrue(elapsedMillis < 200,
                    "判定依据：慢会话等待不得拖住其他会话的发布/拉取");
        } finally {
            pool.shutdown();
            pool.awaitTermination(1, TimeUnit.SECONDS);
        }
    }

    @Test
    void concurrentPublishPollAckDoNotCorruptOrderOrPositions() throws Exception {
        // 缓冲给足：本用例验证并发交错下序号/位点不串位，不验证背压
        MessageBus bus = newBus(5000, 20000);
        String publisher = openAllTopics(bus, "publisher");
        String consumer = openAllTopics(bus, "consumer");
        bus.subscribe(consumer, TOPIC_A);

        int producers = 4;
        int perProducer = 200;
        long maxSeq = (long) producers * perProducer;
        ExecutorService pool = Executors.newFixedThreadPool(producers + 2);
        CountDownLatch start = new CountDownLatch(1);
        AtomicLong failure = new AtomicLong();
        java.util.concurrent.atomic.AtomicReference<Throwable> error =
                new java.util.concurrent.atomic.AtomicReference<>();
        try {
            // 4 个生产者并发发布，序号只能由总线保证主题内唯一递增
            for (int p = 0; p < producers; p++) {
                pool.submit(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < perProducer; i++) {
                            bus.publish(publisher, TOPIC_A, payload("x"));
                        }
                    } catch (Throwable t) {
                        error.compareAndSet(null, t);
                    }
                });
            }
            // 1 个消费者并发轮询 + 累积确认：首投必须严格连续，不允许重排
            java.util.concurrent.Future<?> consumerTask = pool.submit(() -> {
                try {
                    long expect = 1L;
                    long lastAcked = 0L;
                    long idleNanos = 0L;
                    while (expect <= maxSeq) {
                        List<Delivery> batch = bus.poll(consumer, TOPIC_A, 16, 50L, null);
                        if (batch.isEmpty()) {
                            idleNanos += 50_000_000L;
                            if (idleNanos > TimeUnit.SECONDS.toNanos(10)) {
                                throw new AssertionError("消费者长时间无消息，疑似死锁");
                            }
                            continue;
                        }
                        idleNanos = 0L;
                        for (Delivery d : batch) {
                            long seq = d.message().seq();
                            if (!d.redelivered()) {
                                assertEquals(expect, seq, "判定依据：首投序号必须连续不串位");
                                expect++;
                            }
                            // 累积确认：只按递增序号推进位点
                            if (seq > lastAcked) {
                                bus.ack(consumer, TOPIC_A, seq);
                                lastAcked = seq;
                            }
                            if (expect > maxSeq) {
                                break;
                            }
                        }
                    }
                    failure.set(lastAcked);
                } catch (Throwable t) {
                    error.compareAndSet(null, t);
                }
            });
            start.countDown();
            consumerTask.get(15, TimeUnit.SECONDS);
            if (error.get() != null) {
                throw new AssertionError("并发用例出现异常", error.get());
            }
            assertEquals(maxSeq, failure.get(),
                    "判定依据：确认位点最终推进到最大序号，无串位丢失");
            assertTrue(bus.poll(consumer, TOPIC_A, 100).isEmpty(),
                    "判定依据：全部确认后无残留投递");
        } finally {
            pool.shutdownNow();
        }
    }
}
