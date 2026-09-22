package com.github.xingjianluo.robotpubsubbusjava.tests;

import com.github.xingjianluo.robotpubsubbusjava.bus.BusException;
import com.github.xingjianluo.robotpubsubbusjava.bus.ErrorCode;
import com.github.xingjianluo.robotpubsubbusjava.bus.MessageBus;
import com.github.xingjianluo.robotpubsubbusjava.bus.SessionIdentity;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 越权访问与统一失败语义：未授权发布/订阅、未知主题、畸形报文、
 * 超限报文、未知会话均以可区分的错误码显式拒绝，不静默失败。
 */
class AuthorizationTest extends BusTestSupport {

    @Test
    void unauthorizedAndMalformedFailuresAreDistinguishable() {
        MessageBus bus = newBus(100, 1_000, 3);
        // telemetryOnly 仅被授权遥测主题，无权操作指令主题
        String limited = bus.openSession(new SessionIdentity(
                "telemetry-only", Set.of("robot.telemetry"), Set.of("robot.telemetry")));
        log.info("[会话开启] sessionId={} 授权范围=robot.telemetry", limited);

        BusException subEx = assertThrows(BusException.class,
                () -> bus.subscribe(limited, TOPIC));
        log.info("[拒绝] sessionId={} 错误码={} 判定依据=未授权订阅指令主题", limited, subEx.code());
        assertEquals(ErrorCode.UNAUTHORIZED_SUBSCRIBE, subEx.code());

        BusException pubEx = assertThrows(BusException.class,
                () -> bus.publish(limited, TOPIC, payload("cmd")));
        log.info("[拒绝] sessionId={} 错误码={} 判定依据=未授权发布指令主题", limited, pubEx.code());
        assertEquals(ErrorCode.UNAUTHORIZED_PUBLISH, pubEx.code());

        String full = openSession(bus, "full-access");

        BusException unknownTopic = assertThrows(BusException.class,
                () -> bus.publish(full, "robot.unknown", payload("x")));
        log.info("[拒绝] sessionId={} 错误码={} 判定依据=主题未注册", full, unknownTopic.code());
        assertEquals(ErrorCode.UNKNOWN_TOPIC, unknownTopic.code());

        BusException malformed = assertThrows(BusException.class,
                () -> bus.publish(full, TOPIC, new byte[0]));
        log.info("[拒绝] sessionId={} 错误码={} 判定依据=空报文", full, malformed.code());
        assertEquals(ErrorCode.MALFORMED_MESSAGE, malformed.code());

        BusException tooLarge = assertThrows(BusException.class,
                () -> bus.publish(full, TOPIC, new byte[2048]));
        log.info("[拒绝] sessionId={} 错误码={} 判定依据=超过 1024 字节上限", full, tooLarge.code());
        assertEquals(ErrorCode.MESSAGE_TOO_LARGE, tooLarge.code());

        BusException unknownSession = assertThrows(BusException.class,
                () -> bus.reconnect("no-such-session"));
        log.info("[拒绝] 错误码={} 判定依据=重连未知会话", unknownSession.code());
        assertEquals(ErrorCode.SESSION_NOT_FOUND, unknownSession.code());
    }
}
