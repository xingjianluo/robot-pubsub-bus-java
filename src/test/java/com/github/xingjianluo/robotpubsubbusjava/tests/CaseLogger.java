package com.github.xingjianluo.robotpubsubbusjava.tests;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 用例日志：统一打印会话标识、消息序号、确认位点与判定依据，便于人工核对投递语义。
 */
final class CaseLogger {

    private final Logger log;

    private CaseLogger(Class<?> owner) {
        this.log = LoggerFactory.getLogger(owner);
    }

    static CaseLogger of(Class<?> owner) {
        return new CaseLogger(owner);
    }

    void info(String pattern, Object... args) {
        log.info(pattern, args);
    }

    void delivery(String sessionId, long seq, String attempt, int attemptCount, long committed) {
        log.info("[判定] session={} 收到消息 seq={} 投递类型={} 第{}次投递 当前确认位点={} 判定依据=按主题顺序交付",
                sessionId, seq, attempt, attemptCount, committed);
    }

    void ack(String sessionId, String topic, long seq, long committedAfter) {
        log.info("[判定] session={} 确认 topic={} seq={} 确认后位点={} 判定依据=累计ACK,<=位点不再重复交付",
                sessionId, topic, seq, committedAfter);
    }

    void verdict(String sessionId, String what, boolean ok, String basis) {
        log.info("[判定] session={} {} 结果={} 判定依据={}", sessionId, what, ok ? "PASS" : "FAIL", basis);
    }

    void event(String sessionId, String event, long seq, String basis) {
        log.info("[判定] session={} 事件={} seq={} 判定依据={}", sessionId, event, seq, basis);
    }
}
