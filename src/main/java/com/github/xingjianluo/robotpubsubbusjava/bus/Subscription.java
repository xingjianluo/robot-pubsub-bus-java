package com.github.xingjianluo.robotpubsubbusjava.bus;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 单个订阅的运行时状态：
 * <ul>
 *   <li>pending：有界积压队列，达到上限时丢弃最旧消息并记录事件；</li>
 *   <li>unacked：已投递未确认消息及其确认截止时间，超时后重投；</li>
 *   <li>lastAckedSeq：累积确认位点，断线续传从该位点之后继续；</li>
 *   <li>consecutiveDrops：连续丢弃计数，超限则订阅被判定为慢消费者并断开。</li>
 * </ul>
 */
final class Subscription {

    /** 一次未确认投递的跟踪信息。 */
    private static final class InFlight {
        final Message message;
        long deadlineMillis;
        int attempt;

        InFlight(Message message, long deadlineMillis) {
            this.message = message;
            this.deadlineMillis = deadlineMillis;
            this.attempt = 1;
        }
    }

    final String sessionId;
    final String topic;

    private final Deque<Message> pending = new ArrayDeque<>();
    private final Map<Long, InFlight> unacked = new LinkedHashMap<>();
    private long lastAckedSeq = 0L;
    private int consecutiveDrops = 0;
    private boolean evicted = false;

    Subscription(String sessionId, String topic) {
        this.sessionId = sessionId;
        this.topic = topic;
    }

    boolean isEvicted() {
        return evicted;
    }

    long lastAckedSeq() {
        return lastAckedSeq;
    }

    int pendingCount() {
        return pending.size();
    }

    /**
     * 入队新消息。积压满时丢弃队首（最旧）消息并返回对应丢弃事件；
     * 连续丢弃达到上限时订阅被断开，返回断开事件。
     */
    DropEvent enqueue(Message message, BusConfig config) {
        if (evicted) {
            return null;
        }
        if (message.seq() <= lastAckedSeq) {
            return null; // 已确认位点之前的消息不再进入投递
        }
        if (pending.size() < config.maxBacklogPerSubscription()) {
            pending.addLast(message);
            return null;
        }
        Message dropped = pending.pollFirst();
        consecutiveDrops++;
        if (consecutiveDrops >= config.maxDropsBeforeDisconnect()) {
            evicted = true;
            pending.clear();
            return new DropEvent(sessionId, topic, dropped.seq(),
                    DropReason.SLOW_CONSUMER_EVICTED,
                    "连续丢弃 " + consecutiveDrops + " 条，订阅被断开");
        }
        pending.addLast(message);
        return new DropEvent(sessionId, topic, dropped.seq(),
                DropReason.BACKLOG_OVERFLOW,
                "积压达到上限 " + config.maxBacklogPerSubscription() + "，丢弃最旧消息");
    }

    /** 收集已超时未确认、需要重投的消息（按序号升序）。 */
    List<Message> dueRedeliveries(long nowMillis) {
        List<Message> due = new ArrayList<>();
        for (InFlight inFlight : unacked.values()) {
            if (inFlight.deadlineMillis <= nowMillis) {
                due.add(inFlight.message);
            }
        }
        return due;
    }

    /** 取出最多 max 条待投递新消息（按序）。 */
    List<Message> takePending(int max) {
        List<Message> out = new ArrayList<>(Math.min(max, pending.size()));
        while (!pending.isEmpty() && out.size() < max) {
            out.add(pending.pollFirst());
        }
        return out;
    }

    /** 记录一次投递：首次投递建立跟踪，重投刷新截止时间与次数。 */
    void markDelivered(Message message, long nowMillis, long ackTimeoutMillis, boolean redelivery) {
        InFlight inFlight = unacked.get(message.seq());
        if (inFlight == null) {
            unacked.put(message.seq(), new InFlight(message, nowMillis + ackTimeoutMillis));
        } else {
            inFlight.attempt++;
            inFlight.deadlineMillis = nowMillis + ackTimeoutMillis;
        }
    }

    /** 当前投递次数（用于构造 Delivery.attempt）。 */
    int attemptOf(long seq) {
        InFlight inFlight = unacked.get(seq);
        return inFlight == null ? 0 : inFlight.attempt;
    }

    /** 累积确认：位点推进到 seq（含），清除对应未确认跟踪。 */
    void ackUpTo(long seq) {
        if (seq > lastAckedSeq) {
            lastAckedSeq = seq;
        }
        unacked.keySet().removeIf(s -> s <= seq);
    }

    /** 订阅恢复投递（如重连后），清零连续丢弃计数。 */
    void resetDrops() {
        consecutiveDrops = 0;
    }

    /** 断连时将所有未确认投递置为立即到期，重连后优先重投。 */
    void requeueInFlight() {
        for (InFlight inFlight : unacked.values()) {
            inFlight.deadlineMillis = 0L;
        }
    }
}
