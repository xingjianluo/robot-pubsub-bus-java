package com.github.xingjianluo.robotpubsubbusjava.bus;

import java.util.ArrayList;
import java.util.List;

/**
 * 对客户端可见的总线事件。
 *
 * <p>{@code fromSeq}/{@code toSeq} 含义随 {@link EventKind} 而定：
 * <ul>
 *   <li>DELIVERED：被投递消息的序号（fromSeq == toSeq）；</li>
 *   <li>GAP：被丢弃的连续序号区间 [fromSeq, toSeq]，{@link #missingSeqs()} 列出缺口；</li>
 *   <li>SUBSCRIPTION_EVICTED / SESSION_CLOSED：不涉及序号时为 0。</li>
 * </ul>
 */
public record BusEvent(String sessionId, String topic, EventKind kind,
                       long fromSeq, long toSeq, String detail) {

    public static BusEvent delivered(String sessionId, String topic, long seq) {
        return new BusEvent(sessionId, topic, EventKind.DELIVERED, seq, seq, "正常投递");
    }

    public static BusEvent gap(String sessionId, String topic, long fromSeq, long toSeq, String detail) {
        return new BusEvent(sessionId, topic, EventKind.GAP, fromSeq, toSeq, detail);
    }

    public static BusEvent subscriptionEvicted(String sessionId, String topic, String detail) {
        return new BusEvent(sessionId, topic, EventKind.SUBSCRIPTION_EVICTED, 0, 0, detail);
    }

    public static BusEvent sessionClosed(String sessionId, String detail) {
        return new BusEvent(sessionId, null, EventKind.SESSION_CLOSED, 0, 0, detail);
    }

    /** GAP 事件缺失的序号列表；其他类别返回空列表。 */
    public List<Long> missingSeqs() {
        List<Long> missing = new ArrayList<>();
        if (kind == EventKind.GAP) {
            for (long seq = fromSeq; seq <= toSeq; seq++) {
                missing.add(seq);
            }
        }
        return missing;
    }
}
