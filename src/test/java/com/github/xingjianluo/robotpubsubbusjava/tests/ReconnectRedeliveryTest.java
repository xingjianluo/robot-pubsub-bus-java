package com.github.xingjianluo.robotpubsubbusjava.tests;

import com.github.xingjianluo.robotpubsubbusjava.bus.api.BusSession;
import com.github.xingjianluo.robotpubsubbusjava.bus.engine.LocalMessageBus;
import com.github.xingjianluo.robotpubsubbusjava.bus.model.DeliveryAttempt;
import com.github.xingjianluo.robotpubsubbusjava.bus.model.SessionEvent;
import com.github.xingjianluo.robotpubsubbusjava.bus.model.StartPosition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 场景一：断线重连与重复投递。
 * 覆盖：有序投递、已确认消息重连不重复、未确认消息超时重投（RETRY）并最终去重。
 */
class ReconnectRedeliveryTest {

    private final CaseLogger log = CaseLogger.of(ReconnectRedeliveryTest.class);
    private LocalMessageBus bus;

    @AfterEach
    void tearDown() {
        if (bus != null) {
            bus.close();
        }
    }

    private SessionEvent.Delivery take(BusSession session) throws InterruptedException {
        SessionEvent event = session.poll(2000);
        assertTrue(event instanceof SessionEvent.Delivery,
                "期望投递事件，实际: " + event);
        return (SessionEvent.Delivery) event;
    }

    @Test
    void orderedDeliveryAndResumeFromCommittedOffset() throws Exception {
        var props = TestBuses.defaultProps();
        props.setAckTimeoutMillis(50000); // 本用例只验断线重连，隔离超时重投
        bus = TestBuses.newBus(props);
        var auth = new com.github.xingjianluo.robotpubsubbusjava.bus.auth.ConfigAuthorizationService(props);
        var commander = bus.connect(auth.authenticate(TestBuses.TOK_COMMANDER), null);
        var robot = bus.connect(auth.authenticate(TestBuses.TOK_ROBOT), null);

        robot.subscribe(TestBuses.TOPIC_CMD, StartPosition.EARLIEST);
        String resumeToken = robot.resumeToken().encode();
        log.info("[准备] session={} 订阅 topic={} 续传令牌={}",
                robot.sessionId(), TestBuses.TOPIC_CMD, resumeToken);

        // 单在途窗口：逐条发布-接收-确认 seq1..3，确认后窗口才滑到下一条
        for (int i = 1; i <= 3; i++) {
            var result = commander.publish(TestBuses.TOPIC_CMD, TestBuses.payload("cmd-" + i));
            log.info("[准备] session={} 发布消息 seq={}", commander.sessionId(), result.seq());
            var delivery = take(robot);
            assertEquals(i, delivery.message().seq());
            assertEquals(DeliveryAttempt.FIRST, delivery.deliveryAttempt(),
                    "首次投递必须标记 FIRST");
            log.delivery(robot.sessionId(), delivery.message().seq(),
                    delivery.deliveryAttempt().name(), delivery.attemptCount(), i - 1L);
            robot.ack(TestBuses.TOPIC_CMD, i);
            log.ack(robot.sessionId(), TestBuses.TOPIC_CMD, i, i);
        }

        // seq4：接收但不确认（在途）；seq5：窗口被占，仅入待发队列，断线前不会收到
        var r4 = commander.publish(TestBuses.TOPIC_CMD, TestBuses.payload("cmd-4"));
        assertEquals(4L, r4.seq());
        var d4 = take(robot);
        assertEquals(4L, d4.message().seq());
        var r5 = commander.publish(TestBuses.TOPIC_CMD, TestBuses.payload("cmd-5"));
        assertEquals(5L, r5.seq());
        log.info("[准备] seq4 在途不确认，seq5 在待发队列");

        // 断线：seq4 在途、seq5 待发，均未确认
        robot.disconnect();
        log.info("[准备] session={} 主动断线，未确认序号=[4,5]", robot.sessionId());
        assertTrue(robot.closed());

        // 断线期间再发布 seq6，重连后应从位点 3 之后按 4,5,6 续上
        var offline = commander.publish(TestBuses.TOPIC_CMD, TestBuses.payload("cmd-offline"));
        log.info("[准备] 断线期间发布消息 seq={}", offline.seq());
        assertEquals(6L, offline.seq());

        // 重连：从已确认位点 3 续传，seq 1-3 不得重复
        var robot2 = bus.connect(auth.authenticate(TestBuses.TOK_ROBOT), resumeToken);
        log.info("[准备] session={} 以令牌重连，旧句柄={}", robot2.sessionId(), robot.sessionId());

        List<SessionEvent.Delivery> resumedDeliveries = new ArrayList<>();
        List<Long> resumed = new ArrayList<>();
        long[] expect = {4L, 5L, 6L};
        for (int i = 0; i < 3; i++) {
            var delivery = take(robot2);
            resumedDeliveries.add(delivery);
            resumed.add(delivery.message().seq());
            assertEquals(expect[i], delivery.message().seq(), "重连后必须严格有序");
            log.delivery(robot2.sessionId(), delivery.message().seq(),
                    delivery.deliveryAttempt().name(), delivery.attemptCount(), 3);
            robot2.ack(TestBuses.TOPIC_CMD, expect[i]); // 滑动单在途窗口
        }
        assertEquals(List.of(4L, 5L, 6L), resumed,
                "重连后必须仅从已确认位点之后续传，已确认的 1-3 不得重复");
        log.verdict(robot2.sessionId(), "已确认消息不重复交付",
                resumed.equals(List.of(4L, 5L, 6L)), "累计ACK位点=3，续传从 seq=4 开始");
        // seq4 断线前已投递过一次 -> RETRY；seq5 当时被在途窗口阻塞、seq6 断线后产生 -> FIRST
        assertEquals(DeliveryAttempt.RETRY, resumedDeliveries.get(0).deliveryAttempt());
        assertEquals(DeliveryAttempt.FIRST, resumedDeliveries.get(1).deliveryAttempt());
        assertEquals(DeliveryAttempt.FIRST, resumedDeliveries.get(2).deliveryAttempt());
        log.verdict(robot2.sessionId(), "首次投递与重投可区分", true,
                "seq=4 为 RETRY（第2次），seq=5,6 为 FIRST（第1次）");

        // 旧句柄在断代后必须失效（防止旧连接重复 ACK/消费）
        boolean oldHandleFailed = false;
        try {
            robot.ack(TestBuses.TOPIC_CMD, 4);
        } catch (com.github.xingjianluo.robotpubsubbusjava.bus.error.BusException ex) {
            oldHandleFailed = true;
            log.event(robot.sessionId(), "OLD_HANDLE_REJECTED", -1,
                    "错误码=" + ex.errorCode() + "，旧连接代际已失效");
        }
        assertTrue(oldHandleFailed, "断线后的旧句柄操作必须被拒绝");
    }

    @Test
    void unackedMessageIsRedeliveredAfterAckTimeoutAndDedupedByAck() throws Exception {
        bus = TestBuses.newBus();
        var auth = new com.github.xingjianluo.robotpubsubbusjava.bus.auth.ConfigAuthorizationService(
                TestBuses.defaultProps());
        var commander = bus.connect(auth.authenticate(TestBuses.TOK_COMMANDER), null);
        var robot = bus.connect(auth.authenticate(TestBuses.TOK_ROBOT), null);
        robot.subscribe(TestBuses.TOPIC_CMD, StartPosition.EARLIEST);

        commander.publish(TestBuses.TOPIC_CMD, TestBuses.payload("cmd-timeout"));

        // 首次投递 FIRST；故意不 ACK
        var first = take(robot);
        assertEquals(1L, first.message().seq());
        assertEquals(DeliveryAttempt.FIRST, first.deliveryAttempt());
        log.delivery(robot.sessionId(), 1, first.deliveryAttempt().name(), first.attemptCount(), 0);

        // 等待 ACK 超时后重投：必须标记 RETRY 且 attemptCount 递增
        SessionEvent.Delivery retry = null;
        long deadline = System.currentTimeMillis() + 3000;
        while (System.currentTimeMillis() < deadline) {
            SessionEvent event = robot.poll(500);
            if (event instanceof SessionEvent.Delivery d
                    && d.message().seq() == 1L
                    && d.deliveryAttempt() == DeliveryAttempt.RETRY) {
                retry = d;
                break;
            }
        }
        assertTrue(retry != null, "未确认消息必须在 ACK 超时后重投");
        log.delivery(robot.sessionId(), 1, retry.deliveryAttempt().name(),
                retry.attemptCount(), 0);
        assertEquals(2, retry.attemptCount(), "重投次数必须递增");

        // 确认后即便再等也不得重复交付（至少一次 + 去重）
        robot.ack(TestBuses.TOPIC_CMD, 1);
        log.ack(robot.sessionId(), TestBuses.TOPIC_CMD, 1, 1);

        SessionEvent afterAck = robot.poll(800);
        assertFalse(afterAck instanceof SessionEvent.Delivery,
                "已确认消息不得再次交付，实际: " + afterAck);
        log.verdict(robot.sessionId(), "ACK 后不再重复交付", true,
                "累计ACK位点=1，重投扫描跳过 <= 1 的消息");
    }
}
