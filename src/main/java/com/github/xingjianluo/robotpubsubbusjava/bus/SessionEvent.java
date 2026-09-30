package com.github.xingjianluo.robotpubsubbusjava.bus;

import java.util.List;

/**
 * 对客户端可见的会话事件。
 *
 * <ul>
 *   <li>{@link SessionEventType#BACKLOG_GAP}：{@code gaps} 为该主题上被丢弃的连续序号区间；</li>
 *   <li>{@link SessionEventType#SUBSCRIPTION_EVICTED}：{@code topic} 为被摘除订阅的主题，
 *       {@code gaps} 可能携带摘除时一并作废的积压序号；</li>
 *   <li>{@link SessionEventType#SESSION_CLOSED}：会话被关闭，{@code topic} 为空串。</li>
 * </ul>
 *
 * @param sessionId 事件所属会话
 * @param type      事件类型
 * @param topic     关联主题（SESSION_CLOSED 时为 ""）
 * @param gaps      被丢弃的连续序号区间，无缺口时为空列表
 * @param detail    人类可读说明
 */
public record SessionEvent(String sessionId,
                           SessionEventType type,
                           String topic,
                           List<SeqGap> gaps,
                           String detail) {

    public SessionEvent {
        gaps = gaps == null ? List.of() : List.copyOf(gaps);
    }

    public static SessionEvent backlogGap(String sessionId, String topic, List<SeqGap> gaps, String detail) {
        return new SessionEvent(sessionId, SessionEventType.BACKLOG_GAP, topic, gaps, detail);
    }

    public static SessionEvent subscriptionEvicted(String sessionId, String topic, List<SeqGap> gaps, String detail) {
        return new SessionEvent(sessionId, SessionEventType.SUBSCRIPTION_EVICTED, topic, gaps, detail);
    }

    public static SessionEvent sessionClosed(String sessionId, String detail) {
        return new SessionEvent(sessionId, SessionEventType.SESSION_CLOSED, "", List.of(), detail);
    }
}
