package com.github.xingjianluo.robotpubsubbusjava.tests;

import com.github.xingjianluo.robotpubsubbusjava.bus.api.BusSession;
import com.github.xingjianluo.robotpubsubbusjava.bus.auth.ConfigAuthorizationService;
import com.github.xingjianluo.robotpubsubbusjava.bus.engine.LocalMessageBus;
import com.github.xingjianluo.robotpubsubbusjava.bus.error.BusErrorCode;
import com.github.xingjianluo.robotpubsubbusjava.bus.error.BusException;
import com.github.xingjianluo.robotpubsubbusjava.bus.model.StartPosition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 场景二：主题级授权与统一失败语义。
 * 覆盖：未认证、发布越权、订阅越权、未知主题、畸形/超限报文、续传令牌越权、会话越权操作。
 * 各类失败错误码彼此可区分，且均以异常（对客户端可见）呈现，不静默断开/丢消息。
 */
class AuthorizationTest {

    private final CaseLogger log = CaseLogger.of(AuthorizationTest.class);
    private LocalMessageBus bus;
    private ConfigAuthorizationService auth;

    @AfterEach
    void tearDown() {
        if (bus != null) {
            bus.close();
        }
    }

    private void expectCode(BusErrorCode expected, Runnable action, String basis) {
        BusException ex = assertThrows(BusException.class, action::run);
        assertEquals(expected, ex.errorCode(),
                "期望错误码 " + expected + "，实际 " + ex.errorCode() + "（" + ex.getMessage() + "）");
        log.info("[判定] 操作被拒绝 错误码={} 原因='{}' 结果=PASS 判定依据={}",
                ex.errorCode(), ex.getMessage(), basis);
    }

    @Test
    void allFailureClassesAreVisibleAndDistinguishable() {
        bus = TestBuses.newBus();
        auth = new ConfigAuthorizationService(TestBuses.defaultProps());
        var observer = bus.connect(auth.authenticate(TestBuses.TOK_OBSERVER), null);
        var commander = bus.connect(auth.authenticate(TestBuses.TOK_COMMANDER), null);
        var robot = bus.connect(auth.authenticate(TestBuses.TOK_ROBOT), null);
        log.info("[准备] 已建立会话 commander={} robot={} observer={}",
                commander.sessionId(), robot.sessionId(), observer.sessionId());

        // 1) 未认证：空/无效令牌
        assertEquals(null, auth.authenticate(null));
        assertEquals(null, auth.authenticate("bad-token"));
        log.info("[判定] 无效令牌认证结果=null 结果=PASS 判定依据=令牌不在服务端身份表");

        // 2) 发布越权：observer 不得发布 robot.cmd
        expectCode(BusErrorCode.PUBLISH_FORBIDDEN,
                () -> observer.publish(TestBuses.TOPIC_CMD, TestBuses.payload("hack")),
                "observer 的可发布主题仅含 robot.telemetry");

        // 3) 订阅越权：observer 不得订阅 robot.cmd
        expectCode(BusErrorCode.SUBSCRIBE_FORBIDDEN,
                () -> observer.subscribe(TestBuses.TOPIC_CMD, StartPosition.EARLIEST),
                "observer 的可订阅主题仅含 robot.telemetry");

        // 4) 未知主题（发布/订阅），须先于授权判定给出明确错误
        expectCode(BusErrorCode.UNKNOWN_TOPIC,
                () -> commander.publish("robot.unknown", TestBuses.payload("x")),
                "主题 robot.unknown 未在服务端注册");
        expectCode(BusErrorCode.UNKNOWN_TOPIC,
                () -> commander.subscribe("robot.unknown", StartPosition.EARLIEST),
                "主题 robot.unknown 未在服务端注册");

        // 5) 畸形报文：null 负载
        expectCode(BusErrorCode.MALFORMED_MESSAGE,
                () -> commander.publish(TestBuses.TOPIC_CMD, null),
                "负载为 null 属于畸形报文");

        // 6) 超限报文
        byte[] oversized = new byte[5000]; // robot.cmd 上限 4096
        expectCode(BusErrorCode.PAYLOAD_TOO_LARGE,
                () -> commander.publish(TestBuses.TOPIC_CMD, oversized),
                "5000 字节 > 主题上限 4096 字节");

        // 7) 越权使用他人会话：observer 拿 robot 的 sessionId 也无法操作
        expectCode(BusErrorCode.RESUME_FORBIDDEN,
                () -> bus.sessionHandle(robot.sessionId(), auth.authenticate(TestBuses.TOK_OBSERVER)),
                "会话归属身份与请求身份不一致");

        // 8) 续传令牌畸形 / 冒名：observer 持自己的身份持续 commander 的会话
        String forged = new com.github.xingjianluo.robotpubsubbusjava.bus.model.ResumeToken(
                commander.sessionId(), "commander").encode();
        expectCode(BusErrorCode.RESUME_FORBIDDEN,
                () -> bus.connect(auth.authenticate(TestBuses.TOK_OBSERVER), forged),
                "令牌声明身份 commander 与连接身份 observer 不匹配");
        expectCode(BusErrorCode.RESUME_FORBIDDEN,
                () -> bus.connect(auth.authenticate(TestBuses.TOK_ROBOT), "@@@not-base64@@@"),
                "令牌无法解码即判为畸形");

        // 9) 活动会话不可被重复接管
        expectCode(BusErrorCode.SESSION_CONFLICT,
                () -> bus.connect(auth.authenticate(TestBuses.TOK_ROBOT), robot.resumeToken().encode()),
                "会话仍有活动连接，重复接管被拒绝");

        // 10) ACK 未订阅的主题 / 负数序号
        expectCode(BusErrorCode.SUBSCRIPTION_NOT_FOUND,
                () -> robot.ack(TestBuses.TOPIC_TELEMETRY, 1),
                "robot 未订阅 robot.telemetry");
        expectCode(BusErrorCode.MALFORMED_MESSAGE,
                () -> {
                    robot.subscribe(TestBuses.TOPIC_CMD, StartPosition.EARLIEST);
                    robot.ack(TestBuses.TOPIC_CMD, -1);
                },
                "ACK 序号为负属于畸形报文");

        // 反证：observer 在自己被授权的主题上可以正常发布（授权失败不波及合法操作）
        var ok = observer.publish(TestBuses.TOPIC_TELEMETRY, TestBuses.payload("temp=36.5"));
        assertEquals(1L, ok.seq());
        assertTrue(observer.sessionId().startsWith("s-"));
        log.verdict(observer.sessionId(), "授权主题内正常发布 seq=" + ok.seq(), true,
                "observer 对 robot.telemetry 有发布权，错误不影响合法操作");
    }
}
