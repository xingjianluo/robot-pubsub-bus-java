package com.github.xingjianluo.robotpubsubbusjava.tests;

import com.github.xingjianluo.robotpubsubbusjava.bus.api.BusSession;
import com.github.xingjianluo.robotpubsubbusjava.bus.auth.ConfigAuthorizationService;
import com.github.xingjianluo.robotpubsubbusjava.bus.engine.LocalMessageBus;
import com.github.xingjianluo.robotpubsubbusjava.bus.error.BusErrorCode;
import com.github.xingjianluo.robotpubsubbusjava.bus.error.BusException;
import com.github.xingjianluo.robotpubsubbusjava.bus.model.BackpressurePolicy;
import com.github.xingjianluo.robotpubsubbusjava.bus.model.DeliveryAttempt;
import com.github.xingjianluo.robotpubsubbusjava.bus.model.SessionEvent;
import com.github.xingjianluo.robotpubsubbusjava.bus.model.StartPosition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 场景三：慢消费者与资源上限保护。
 *
 * <p>投递模型：每订阅严格单在途窗口（inflight<=1）+ 有界 pending（上限 maxInflight-1）。
 * 已发布但尚未到在途窗口的消息进入 pending；积压（inflight+pending）达 maxInflight 触发策略。
 * 分别覆盖 REJECT / DROP_OLDEST / DISCONNECT，并验证慢订阅不拖垮其他订阅；
 * 三类结果（拒绝/丢弃/摘除）原因彼此可区分。
 */
class SlowConsumerTest {

    private static final String TOPIC = "robot.pressure"; // 独立主题，避免与默认配置/超时相互干扰
    private final CaseLogger log = CaseLogger.of(SlowConsumerTest.class);
    private LocalMessageBus bus;
    private ConfigAuthorizationService auth;

    @AfterEach
    void tearDown() {
        if (bus != null) {
            bus.close();
        }
    }

    private BusSession connect(String token) {
        return bus.connect(auth.authenticate(token), null);
    }

    private SessionEvent.Delivery takeDelivery(BusSession session) throws Exception {
        SessionEvent e = session.poll(1000);
        assertTrue(e instanceof SessionEvent.Delivery, "期望投递事件: " + e);
        return (SessionEvent.Delivery) e;
    }

    private void publishSeq(BusSession publisher, int seq) {
        var r = publisher.publish(TOPIC, TestBuses.payload("c" + seq));
        assertEquals(seq, r.seq());
        log.info("[准备] 发布 seq={}", seq);
    }

    @Test
    void rejectPolicyRejectsPublishWithDistinguishableErrorAndIsolatesConsumers() throws Exception {
        var props = TestBuses.defaultProps();
        props.setAckTimeoutMillis(30000); // 慢消费用例隔离超时重投变量
        TestBuses.addTopic(props, TOPIC, BackpressurePolicy.REJECT, 4, 64, 4096);
        bus = TestBuses.newBus(props);
        auth = new ConfigAuthorizationService(props);

        var commander = connect(TestBuses.TOK_COMMANDER);
        var slow = connect(TestBuses.TOK_ROBOT);
        var healthy = connect(TestBuses.TOK_ROBOT);
        slow.subscribe(TOPIC, StartPosition.EARLIEST);
        healthy.subscribe(TOPIC, StartPosition.EARLIEST);
        log.info("[准备] 慢会话={} 健康会话={} 主题={} 策略=REJECT maxInflight=4（单在途+3待发）",
                slow.sessionId(), healthy.sessionId(), TOPIC);

        // seq1..4：慢会话 seq1 在途，seq2..4 入 pending（积压=4 满）；健康会话逐条确认
        for (int i = 1; i <= 4; i++) {
            publishSeq(commander, i);
            var h = takeDelivery(healthy);
            assertEquals(i, h.message().seq());
            healthy.ack(TOPIC, i);
        }
        var s1 = takeDelivery(slow);
        assertEquals(1L, s1.message().seq());
        log.info("[准备] 慢会话仅收到 seq=1 且不确认；seq2-4 在 pending，积压=4");

        // seq5：慢会话满额 -> 整笔拒绝，任何订阅者都收不到 seq5
        BusException rejected = null;
        try {
            commander.publish(TOPIC, TestBuses.payload("c5"));
        } catch (BusException ex) {
            rejected = ex;
        }
        assertNotNull(rejected, "积压达上限时 REJECT 策略必须拒绝发布");
        assertEquals(BusErrorCode.SUBSCRIBER_BACKPRESSURE_REJECTED, rejected.errorCode());
        log.event(slow.sessionId(), "PUBLISH_REJECTED", 5,
                "错误码=" + rejected.errorCode() + "，原因=" + rejected.getMessage());

        // 慢会话逐条确认 seq1..4，窗口滑动，积压下降
        long ack = s1.message().seq();
        slow.ack(TOPIC, ack);
        log.ack(slow.sessionId(), TOPIC, ack, ack);
        for (int expected = 2; expected <= 4; expected++) {
            var d = takeDelivery(slow);
            assertEquals(expected, d.message().seq());
            slow.ack(TOPIC, expected);
            log.ack(slow.sessionId(), TOPIC, expected, expected);
        }

        // 积压清零后发布恢复；seq6 两个订阅者都收到，seq5 对谁都没投递
        var recovered = commander.publish(TOPIC, TestBuses.payload("c6"));
        assertEquals(6L, recovered.seq());
        assertEquals(6L, takeDelivery(slow).message().seq());
        assertEquals(6L, takeDelivery(healthy).message().seq());
        healthy.ack(TOPIC, 6);
        log.verdict(healthy.sessionId(), "慢订阅不拖垮健康订阅，被拒 seq5 不投递、seq6 正常", true,
                "健康订阅全程逐条 ACK 零积压；发布被拒时整笔不扇出");
        log.verdict(slow.sessionId(), "ACK 释放积压后发布恢复且严格有序(1,2,3,4,6)", true,
                "积压有界=4，确认后窗口顺序滑动");
    }

    @Test
    void dropOldestPolicyDropsOldestWithExplicitNotification() throws Exception {
        var props = TestBuses.defaultProps();
        props.setAckTimeoutMillis(30000); // 慢消费用例隔离超时重投变量
        TestBuses.addTopic(props, TOPIC, BackpressurePolicy.DROP_OLDEST, 4, 128, 4096);
        bus = TestBuses.newBus(props);
        auth = new ConfigAuthorizationService(props);

        var commander = connect(TestBuses.TOK_COMMANDER);
        var slow = connect(TestBuses.TOK_ROBOT);
        slow.subscribe(TOPIC, StartPosition.EARLIEST);
        log.info("[准备] 慢会话={} 策略=DROP_OLDEST maxInflight=4", slow.sessionId());

        for (int i = 1; i <= 4; i++) {
            publishSeq(commander, i);
        }
        assertEquals(1L, takeDelivery(slow).message().seq()); // seq1 在途，2-4 pending
        log.info("[准备] seq1 在途不确认，seq2-4 待发，积压=4");

        // seq5 满额：丢弃全局最旧未确认消息 seq1（已投递但未确认），位点顺移
        publishSeq(commander, 5);
        SessionEvent dropped = slow.poll(1000);
        assertTrue(dropped instanceof SessionEvent.Dropped,
                "DROP_OLDEST 必须产生显式 Dropped 通知，实际: " + dropped);
        SessionEvent.Dropped d = (SessionEvent.Dropped) dropped;
        assertEquals(1L, d.fromSeq());
        assertEquals(1L, d.toSeq());
        log.event(slow.sessionId(), "DROPPED_OLDEST", d.fromSeq(),
                "原因=" + d.reason() + "，丢弃区间=[" + d.fromSeq() + "," + d.toSeq() + "]");

        // 位点已顺移到 1，窗口释放 -> seq2 被投递（不再重投 seq1）
        var next = takeDelivery(slow);
        assertEquals(2L, next.message().seq());
        log.verdict(slow.sessionId(), "丢弃 seq1 后投递 seq2，被丢弃消息不重投且积压有界", true,
                "丢弃最旧未确认1条腾出位置，committed 顺移，inflight+pending 恒 <= 4");

        // seq2 在途不确认；积压仍满（seq2 在途 + seq3-5 待发=4）。再发 seq6：
        // 丢弃最旧未确认 seq2（在途），位点顺移到 2，窗口释放后投递 seq3
        publishSeq(commander, 6);
        SessionEvent dropped2 = slow.poll(1000);
        assertTrue(dropped2 instanceof SessionEvent.Dropped,
                "再次满额必须产生 Dropped 通知，实际: " + dropped2);
        assertEquals(2L, ((SessionEvent.Dropped) dropped2).fromSeq());
        log.event(slow.sessionId(), "DROPPED_OLDEST", 2,
                "在途 seq2 未确认即被丢弃，位点顺移至 2");
        assertEquals(3L, takeDelivery(slow).message().seq());
        log.verdict(slow.sessionId(), "发布始终不失败且严格有序(1丢,2投,2丢,3投)", true,
                "DROP_OLDEST 对在途/待发统一取全局最旧，丢弃与投递原因可区分");
    }

    @Test
    void disconnectPolicyEvictsSlowSubscriptionAndAllowsResumableResubscribe() throws Exception {
        var props = TestBuses.defaultProps();
        props.setAckTimeoutMillis(30000); // 慢消费用例隔离超时重投变量
        TestBuses.addTopic(props, TOPIC, BackpressurePolicy.DISCONNECT, 4, 128, 4096);
        bus = TestBuses.newBus(props);
        auth = new ConfigAuthorizationService(props);

        var commander = connect(TestBuses.TOK_COMMANDER);
        var slow = connect(TestBuses.TOK_ROBOT);
        slow.subscribe(TOPIC, StartPosition.EARLIEST);
        String resumeToken = slow.resumeToken().encode();

        // 单在途（maxInflight=4）：seq1 投递并确认；泵取随即把 seq2 置为在途，seq3,4 待发
        for (int i = 1; i <= 4; i++) {
            publishSeq(commander, i);
        }
        assertEquals(1L, takeDelivery(slow).message().seq());
        slow.ack(TOPIC, 1);
        log.ack(slow.sessionId(), TOPIC, 1, 1);
        assertEquals(2L, takeDelivery(slow).message().seq()); // ACK 泵取：seq2 在途
        log.info("[准备] seq2 在途不确认，seq3,4 待发，积压=3");

        // seq5 入队后积压=4（仍有空位判定，正常入待发）；seq6 入队前积压已达上限 -> 触发摘除，
        // 触发消息 seq6 不扇出给休眠订阅（但保留在主题窗口，重订阅可追回）
        publishSeq(commander, 5);
        publishSeq(commander, 6);
        SessionEvent closed = slow.poll(1000);
        assertTrue(closed instanceof SessionEvent.SessionClosed,
                "DISCONNECT 必须产生显式 SessionClosed 事件，实际: " + closed);
        SessionEvent.SessionClosed sc = (SessionEvent.SessionClosed) closed;
        assertEquals(BusErrorCode.SLOW_CONSUMER_DISCONNECTED.name(), sc.reasonCode());
        log.event(slow.sessionId(), "SUBSCRIPTION_EVICTED", -1,
                "错误码=" + sc.reasonCode() + "，订阅=" + sc.subscriptionId() + "，原因=" + sc.message());

        // 订阅级隔离：会话存活、可继续发布；休眠期消息不再扇给该订阅
        assertFalse(slow.closed(), "仅摘除慢订阅，会话不得被连带关闭");
        publishSeq(commander, 7);
        publishSeq(commander, 8);
        log.verdict(slow.sessionId(), "摘除后会话存活且发布不失败", true,
                "摘除作用域为订阅；休眠订阅不在扇出列表，零内存增长");

        // 对被摘除订阅 ACK：可区分错误
        BusException ackErr = null;
        try {
            slow.ack(TOPIC, 2);
        } catch (BusException ex) {
            ackErr = ex;
        }
        assertNotNull(ackErr);
        assertEquals(BusErrorCode.SLOW_CONSUMER_DISCONNECTED, ackErr.errorCode());
        log.event(slow.sessionId(), "ACK_ON_EVICTED_REJECTED", 2,
                "错误码=" + ackErr.errorCode() + "，提示重新订阅续传");

        // 同一连接重新订阅：保留确认位点=1，从保留窗口续投 2..8（摘除前 seq2 已投递->RETRY，
        // seq3-8 从未投递->FIRST）。逐条确认以滑动单在途窗口。
        var resub = slow.subscribe(TOPIC, StartPosition.EARLIEST);
        assertEquals(1L, resub.committedSeq());
        List<Long> seqs = new ArrayList<>();
        List<DeliveryAttempt> kinds = new ArrayList<>();
        for (long expected = 2; expected <= 8; expected++) {
            var d = takeDelivery(slow);
            seqs.add(d.message().seq());
            kinds.add(d.deliveryAttempt());
            assertEquals(expected, d.message().seq(), "必须严格有序");
            slow.ack(TOPIC, expected); // 确认以滑动窗口，使下一条可投递
        }
        assertEquals(List.of(2L, 3L, 4L, 5L, 6L, 7L, 8L), seqs, "必须严格有序且无缺口");
        assertEquals(DeliveryAttempt.RETRY, kinds.get(0), "seq2 摘除前已投递，应为重投");
        for (int i = 1; i < kinds.size(); i++) {
            assertEquals(DeliveryAttempt.FIRST, kinds.get(i),
                    "seq=" + seqs.get(i) + " 从未投递，应为首次");
        }
        log.verdict(slow.sessionId(), "重新订阅从位点=1严格有序续传，首投/重投可区分", true,
                "seq2=RETRY；seq3-8=FIRST；已确认 seq1 不重复");

        // 断线重连路径同样可续传且不重复
        slow.ack(TOPIC, 8);
        slow.disconnect();
        var resumed = bus.connect(auth.authenticate(TestBuses.TOK_ROBOT), resumeToken);
        SessionEvent extra = resumed.poll(300);
        assertFalse(extra instanceof SessionEvent.Delivery,
                "全部确认后重连不得重复交付，实际: " + extra);
        log.verdict(resumed.sessionId(), "重连不重复交付已确认消息", true,
                "确认位点=8，重连后无待投递");
    }
}
