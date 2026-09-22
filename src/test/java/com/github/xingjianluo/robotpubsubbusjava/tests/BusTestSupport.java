package com.github.xingjianluo.robotpubsubbusjava.tests;

import com.github.xingjianluo.robotpubsubbusjava.bus.BusConfig;
import com.github.xingjianluo.robotpubsubbusjava.bus.Delivery;
import com.github.xingjianluo.robotpubsubbusjava.bus.MessageBus;
import com.github.xingjianluo.robotpubsubbusjava.bus.SessionIdentity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;

/** 测试公共支撑：构造总线/会话，并按要求打印会话标识、消息序号、确认位点与判定依据。 */
abstract class BusTestSupport {

    protected final Logger log = LoggerFactory.getLogger(getClass());
    protected final MutableClock clock = new MutableClock();

    protected static final String TOPIC = "robot.commands";

    protected MessageBus newBus(int maxBacklog, long ackTimeoutMillis, int maxDrops) {
        MessageBus bus = new MessageBus(new BusConfig(maxBacklog, ackTimeoutMillis, 1024, maxDrops), clock);
        bus.registerTopic(TOPIC);
        return bus;
    }

    protected String openSession(MessageBus bus, String clientId) {
        String sessionId = bus.openSession(new SessionIdentity(clientId, Set.of(TOPIC), Set.of(TOPIC)));
        log.info("[会话开启] sessionId={} clientId={}", sessionId, clientId);
        return sessionId;
    }

    protected void logDeliveries(String sessionId, List<Delivery> deliveries, String verdict) {
        for (Delivery d : deliveries) {
            log.info("[投递] sessionId={} seq={} redelivered={} attempt={} 判定依据={}",
                    sessionId, d.message().seq(), d.redelivered(), d.attempt(), verdict);
        }
    }

    protected void logAck(String sessionId, MessageBus bus, long seq) {
        log.info("[确认] sessionId={} ackSeq={} 当前确认位点={}",
                sessionId, seq, bus.ackedPosition(sessionId, TOPIC));
    }

    protected static byte[] payload(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }
}
