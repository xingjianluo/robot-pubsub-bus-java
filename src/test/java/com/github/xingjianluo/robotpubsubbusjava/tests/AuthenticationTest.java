package com.github.xingjianluo.robotpubsubbusjava.tests;

import com.github.xingjianluo.robotpubsubbusjava.bus.BusConfig;
import com.github.xingjianluo.robotpubsubbusjava.bus.BusException;
import com.github.xingjianluo.robotpubsubbusjava.bus.ErrorCode;
import com.github.xingjianluo.robotpubsubbusjava.bus.MessageBus;
import com.github.xingjianluo.robotpubsubbusjava.bus.SessionIdentity;
import com.github.xingjianluo.robotpubsubbusjava.bus.StaticTokenAuthenticator;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 建会话身份核验：凭据必须由服务端认下并核验为身份，客户端不能自称身份；
 * 未携带凭据与凭据非法被直接拒绝，且原因与越权、未知主题可区分。
 */
class AuthenticationTest extends BusTestSupport {

    private MessageBus newAuthenticatedBus() {
        MessageBus bus = new MessageBus(new BusConfig(100, 1_000, 1024, 3), clock,
                new StaticTokenAuthenticator(Map.of(
                        "robot-token", new SessionIdentity("robot", Set.of(TOPIC), Set.of(TOPIC)),
                        "telemetry-token", new SessionIdentity("telemetry",
                                Set.of("robot.telemetry"), Set.of("robot.telemetry")))));
        bus.registerTopic(TOPIC);
        bus.registerTopic("robot.telemetry");
        return bus;
    }

    @Test
    void sessionIdentityIsAuthenticatedByServer() {
        MessageBus bus = newAuthenticatedBus();

        BusException missing = assertThrows(BusException.class, () -> bus.openSession((String) null));
        log.info("[拒绝] 错误码={} 判定依据=未携带凭据", missing.code());
        assertEquals(ErrorCode.UNAUTHENTICATED, missing.code());

        BusException blank = assertThrows(BusException.class, () -> bus.openSession("  "));
        assertEquals(ErrorCode.UNAUTHENTICATED, blank.code());

        BusException invalid = assertThrows(BusException.class, () -> bus.openSession("forged-token"));
        log.info("[拒绝] 错误码={} 判定依据=凭据不被服务端认可", invalid.code());
        assertEquals(ErrorCode.AUTHENTICATION_FAILED, invalid.code());

        // 合法凭据：服务端认下身份，客户端不能自称是谁就拿到谁的权限
        String telemetry = bus.openSession("telemetry-token");
        log.info("[会话开启] sessionId={} 凭据=telemetry-token 服务端认下身份=telemetry", telemetry);
        assertEquals("telemetry", bus.clientId(telemetry));

        BusException forged = assertThrows(BusException.class,
                () -> bus.publish(telemetry, TOPIC, payload("cmd")));
        log.info("[拒绝] sessionId={} 错误码={} 判定依据=令牌只被授权遥测主题，越权与未认证可区分",
                telemetry, forged.code());
        assertEquals(ErrorCode.UNAUTHORIZED_PUBLISH, forged.code());

        BusException unknownTopic = assertThrows(BusException.class,
                () -> bus.publish(telemetry, "robot.unknown", payload("x")));
        assertEquals(ErrorCode.UNKNOWN_TOPIC, unknownTopic.code());

        String robot = bus.openSession("robot-token");
        long seq = bus.publish(robot, TOPIC, payload("cmd-1"));
        assertTrue(seq > 0, "判定依据：合法凭据建会话后可正常发布");
    }
}
