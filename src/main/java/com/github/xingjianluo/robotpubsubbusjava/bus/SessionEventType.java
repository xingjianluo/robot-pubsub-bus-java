package com.github.xingjianluo.robotpubsubbusjava.bus;

/**
 * 客户端可见的会话事件类型，三类必须彼此区分：
 * <ul>
 *   <li>{@link #BACKLOG_GAP}：因背压丢了一段序号，客户端“这段没收到”；</li>
 *   <li>{@link #SUBSCRIPTION_EVICTED}：订阅被摘除，客户端“这个订阅没了”，会话仍在；</li>
 *   <li>{@link #SESSION_CLOSED}：会话被关闭，客户端“这个会话没了”。</li>
 * </ul>
 * 正常投递本身不产生事件，通过 poll 拿到的 {@link Delivery}（含 seq/redelivered/attempt）判定。
 */
public enum SessionEventType {
    /** 订阅积压超限，一段序号被丢弃，gap 给出缺失区间 */
    BACKLOG_GAP,
    /** 订阅按 EVICT_SUBSCRIPTION 策略或慢消费者阈值被摘除，会话保留 */
    SUBSCRIPTION_EVICTED,
    /** 会话被服务端永久关闭 */
    SESSION_CLOSED
}
