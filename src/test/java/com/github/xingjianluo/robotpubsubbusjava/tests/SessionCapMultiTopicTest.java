package com.github.xingjianluo.robotpubsubbusjava.tests;

import com.github.xingjianluo.robotpubsubbusjava.bus.BackpressurePolicy;
import com.github.xingjianluo.robotpubsubbusjava.bus.BusConfig;
import com.github.xingjianluo.robotpubsubbusjava.bus.BusEvent;
import com.github.xingjianluo.robotpubsubbusjava.bus.Delivery;
import com.github.xingjianluo.robotpubsubbusjava.bus.EventKind;
import com.github.xingjianluo.robotpubsubbusjava.bus.MessageBus;
import com.github.xingjianluo.robotpubsubbusjava.bus.SessionIdentity;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 多订阅共享会话总上限的边界场景：一个主题占满会话总量、另一主题一条都还没积压时，
 * 继续往空主题发布不得报服务端错误，必须按该订阅选择的背压策略正常处理
 * （丢最旧或摘订阅），缺口事件完整反映丢了哪些序号，且不连累其他订阅者。
 */
class SessionCapMultiTopicTest extends BusTestSupport {

    private static final String TOPIC_B = "robot.telemetry";

    private MessageBus newCappedBus(int perSubscription, int perSession, int maxDrops) {
        MessageBus bus = new MessageBus(
                new BusConfig(perSubscription, 60_000, 1024, maxDrops, perSession, 200), clock);
        bus.registerTopic(TOPIC);
        bus.registerTopic(TOPIC_B);
        return bus;
    }

    private String openBothTopicsSession(MessageBus bus, String clientId) {
        String sessionId = bus.openSession(new SessionIdentity(
                clientId, Set.of(TOPIC, TOPIC_B), Set.of(TOPIC, TOPIC_B)));
        log.info("[会话开启] sessionId={} clientId={} 授权范围=两主题", sessionId, clientId);
        return sessionId;
    }

    @Test
    void publishToEmptyTopicWhenSessionCapFilledByOtherTopicDoesNotFail() {
        MessageBus bus = newCappedBus(10, 3, 100); // 单订阅 10，会话总量 3，丢弃阈值放宽
        String publisher = openBothTopicsSession(bus, "publisher");
        String slow = openBothTopicsSession(bus, "slow-consumer");
        String peer = openBothTopicsSession(bus, "peer-consumer");
        bus.subscribe(slow, TOPIC);
        bus.subscribe(slow, TOPIC_B);
        bus.subscribe(peer, TOPIC_B);

        // 主题 A 积压 3 条，把会话总上限占满；主题 B 一条都没有
        for (int i = 1; i <= 3; i++) {
            long seq = bus.publish(publisher, TOPIC, payload("cmd-" + i));
            assertEquals(i, seq);
        }
        assertEquals(3, bus.totalPending(slow));
        assertEquals(0, bus.pendingCount(slow, TOPIC_B),
                "前置条件：空主题尚无积压");

        // 再往空主题 B 发：曾经这里直接 NPE -> 500；现在必须正常返回序号
        long bSeq1 = bus.publish(publisher, TOPIC_B, payload("tm-1"));
        long bSeq2 = bus.publish(publisher, TOPIC_B, payload("tm-2"));
        log.info("[发布] 会话总量被主题A占满时向空主题B发布，序号={},{} 判定依据=发布正常返回不报错",
                bSeq1, bSeq2);
        assertEquals(1L, bSeq1);
        assertEquals(2L, bSeq2);

        // 空主题无积压可丢，两条新消息都只能成为缺口，总量不突破上限，主题 A 不受影响
        assertTrue(bus.totalPending(slow) <= 3,
                "判定依据：会话总排队量全程不得突破上限");
        assertEquals(3, bus.pendingCount(slow, TOPIC),
                "判定依据：其他主题的背压处理不得动到本主题积压");
        assertEquals(0, bus.pendingCount(slow, TOPIC_B),
                "判定依据：空主题无空间入队，消息按策略记缺口而非入队");

        List<BusEvent> gaps = bus.drainEvents(slow).stream()
                .filter(e -> e.kind() == EventKind.GAP && e.topic().equals(TOPIC_B))
                .toList();
        List<Long> missing = gaps.stream().flatMap(g -> g.missingSeqs().stream()).toList();
        log.info("[缺口事件] 空主题B缺口序号={} 判定依据=丢了哪些序号由事件完整反映", missing);
        assertEquals(List.of(1L, 2L), missing,
                "判定依据：缺口必须恰好覆盖被丢弃的序号，不多也不少");

        // 其他订阅者不受连累：peer 正常收齐 B 的两条
        List<Delivery> peerGot = bus.poll(peer, TOPIC_B, 10);
        logDeliveries(peer, peerGot, "同主题其他订阅者不受慢会话连累");
        assertEquals(List.of(1L, 2L), peerGot.stream().map(d -> d.message().seq()).toList(),
                "判定依据：单会话容量超限不得掐断该消息对其他订阅者的投递");
    }

    @Test
    void emptyTopicUnderSessionCapHonorsDropOldestWhenItLaterBacklogs() {
        MessageBus bus = newCappedBus(10, 3, 100);
        String publisher = openBothTopicsSession(bus, "publisher");
        String slow = openBothTopicsSession(bus, "slow-consumer");
        bus.subscribe(slow, TOPIC);
        bus.subscribe(slow, TOPIC_B);

        for (int i = 1; i <= 3; i++) {
            bus.publish(publisher, TOPIC, payload("cmd-" + i));
        }
        // 空主题前两条：无积压可丢 -> 新消息自身缺口
        bus.publish(publisher, TOPIC_B, payload("tm-1"));
        bus.publish(publisher, TOPIC_B, payload("tm-2"));
        // 拉走主题A（进入未确认态，不再计入排队量），会话腾出 3 个位置
        bus.poll(slow, TOPIC, 10);
        assertEquals(3L, bus.publish(publisher, TOPIC_B, payload("tm-3")));
        assertEquals(4L, bus.publish(publisher, TOPIC_B, payload("tm-4")));
        assertEquals(5L, bus.publish(publisher, TOPIC_B, payload("tm-5")));

        // 会话容量只余 3：tm-3/4/5 入队，tm-6 时丢弃最旧的 tm-3，tm-6 入队
        assertEquals(6L, bus.publish(publisher, TOPIC_B, payload("tm-6")));
        List<Delivery> bGot = bus.poll(slow, TOPIC_B, 10);
        logDeliveries(slow, bGot, "腾出容量后丢最旧，保留最新三条");
        assertEquals(List.of(4L, 5L, 6L), bGot.stream().map(d -> d.message().seq()).toList());

        List<BusEvent> gaps = bus.drainEvents(slow).stream()
                .filter(e -> e.kind() == EventKind.GAP && e.topic().equals(TOPIC_B))
                .toList();
        List<Long> missing = gaps.stream().flatMap(g -> g.missingSeqs().stream()).toList();
        log.info("[缺口事件] 主题B累计缺口={} 判定依据=空主题自丢与后续丢最旧都对得上", missing);
        assertEquals(List.of(1L, 2L, 3L), missing,
                "判定依据：tm-1,tm-2 因会话容量记缺口，tm-3 后被丢最旧，恰好 1,2,3");
    }

    @Test
    void evictPolicyOnEmptyTopicRemovesSubscriptionButKeepsSessionAndResumes() {
        MessageBus bus = newCappedBus(10, 3, 100);
        String publisher = openBothTopicsSession(bus, "publisher");
        String slow = openBothTopicsSession(bus, "slow-consumer");
        bus.subscribe(slow, TOPIC);
        bus.subscribe(slow, TOPIC_B, BackpressurePolicy.EVICT_SUBSCRIPTION);

        for (int i = 1; i <= 3; i++) {
            bus.publish(publisher, TOPIC, payload("cmd-" + i));
        }
        // 空主题 B 一发即因会话总量超限被摘除：发布正常返回，会话不废
        long seq = bus.publish(publisher, TOPIC_B, payload("tm-1"));
        log.info("[摘除] 空主题B因会话总量超限被摘除，发布序号={} 仍正常返回", seq);

        List<BusEvent> events = bus.drainEvents(slow);
        List<BusEvent> evicted = events.stream()
                .filter(e -> e.kind() == EventKind.SUBSCRIPTION_EVICTED
                        && e.topic().equals(TOPIC_B)).toList();
        List<Long> missing = events.stream()
                .filter(e -> e.kind() == EventKind.GAP && e.topic().equals(TOPIC_B))
                .flatMap(g -> g.missingSeqs().stream()).toList();
        assertEquals(1, evicted.size(), "判定依据：EVICT 策略产生摘除事件");
        assertEquals(List.of(1L), missing, "判定依据：触发摘除的消息序号记为缺口");

        // 订阅被摘除不能作废会话：主题 A 仍可拉取、确认
        List<Delivery> aGot = bus.poll(slow, TOPIC, 10);
        assertEquals(List.of(1L, 2L, 3L), aGot.stream().map(d -> d.message().seq()).toList());
        bus.ack(slow, TOPIC, 3);
        assertEquals(3L, bus.ackedPosition(slow, TOPIC));

        // 重连后从已确认位点续传；A 没有新消息时拉取返回空而不是报错
        bus.disconnect(slow);
        bus.publish(publisher, TOPIC, payload("cmd-4"));
        bus.reconnect(slow);
        List<Delivery> resumed = bus.poll(slow, TOPIC, 10);
        logDeliveries(slow, resumed, "摘除订阅不影响会话，重连后A从确认位点续传");
        assertEquals(List.of(4L), resumed.stream().map(d -> d.message().seq()).toList(),
                "判定依据：订阅摘除保留会话与确认位点，重连续传");
    }

    @Test
    void concurrentPublishAcrossTopicsNeverExceedsSessionCapAndGapsAreExact() throws Exception {
        int perSession = 20;
        MessageBus bus = new MessageBus(
                new BusConfig(1_000, 60_000, 1024, 1_000_000, perSession, 50), clock);
        bus.registerTopic(TOPIC);
        bus.registerTopic(TOPIC_B);
        String publisher = openBothTopicsSession(bus, "publisher");
        String slow = openBothTopicsSession(bus, "slow-consumer");
        bus.subscribe(slow, TOPIC);
        bus.subscribe(slow, TOPIC_B);

        int perTopic = 200;
        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        List<Integer> observedTotals = new CopyOnWriteArrayList<>();
        AtomicLong observedMax = new AtomicLong();
        try {
            Future<?> f1 = pool.submit(() -> {
                await(start);
                for (int i = 0; i < perTopic; i++) {
                    bus.publish(publisher, TOPIC, payload("c"));
                }
            });
            Future<?> f2 = pool.submit(() -> {
                await(start);
                for (int i = 0; i < perTopic; i++) {
                    bus.publish(publisher, TOPIC_B, payload("t"));
                }
            });
            Future<?> watcher = pool.submit(() -> {
                await(start);
                for (int i = 0; i < 2_000; i++) {
                    int total = bus.totalPending(slow);
                    observedTotals.add(total);
                    observedMax.accumulateAndGet(total, Math::max);
                    if (total > perSession) {
                        throw new AssertionError("会话总积压突破上限：" + total);
                    }
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            });
            start.countDown();
            f1.get(30, TimeUnit.SECONDS);
            f2.get(30, TimeUnit.SECONDS);
            watcher.get(30, TimeUnit.SECONDS);
        } finally {
            pool.shutdownNow();
        }

        log.info("[并发] 观察到会话总积压峰值={} 上限={} 采样={} 判定依据=并发交错不突破上限",
                observedMax.get(), perSession, observedTotals.size());
        assertTrue(observedMax.get() <= perSession,
                "判定依据：两个主题并发发布期间会话总排队量不得突破上限");

        // 最终记账核对：两主题各自 = 已投递无（从不拉取），积压 + 缺口 = 发布总数
        List<Delivery> aLeft = bus.poll(slow, TOPIC, 1_000);
        List<Delivery> bLeft = bus.poll(slow, TOPIC_B, 1_000);
        List<BusEvent> allEvents = bus.drainEvents(slow);
        long aMissing = allEvents.stream()
                .filter(e -> e.kind() == EventKind.GAP && e.topic().equals(TOPIC))
                .flatMapToLong(g -> g.missingSeqs().stream().mapToLong(Long::longValue))
                .distinct().count();
        long bMissing = allEvents.stream()
                .filter(e -> e.kind() == EventKind.GAP && e.topic().equals(TOPIC_B))
                .flatMapToLong(g -> g.missingSeqs().stream().mapToLong(Long::longValue))
                .distinct().count();
        log.info("[对账] A: 剩余积压{} + 缺口{} = {}；B: 剩余积压{} + 缺口{} = {}",
                aLeft.size(), aMissing, aLeft.size() + aMissing,
                bLeft.size(), bMissing, bLeft.size() + bMissing);
        assertEquals(perTopic, aLeft.size() + aMissing,
                "判定依据：主题A 积压条数与缺口序号之和等于发布总数，不多不少");
        assertEquals(perTopic, bLeft.size() + bMissing,
                "判定依据：主题B 积压条数与缺口序号之和等于发布总数，不多不少");
        assertFalse(aLeft.isEmpty() && bLeft.isEmpty(), "判定依据：容量内消息应保留为积压");
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
