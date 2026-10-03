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
 *   <li>pending：有界积压队列，超限按订阅的 {@link BackpressurePolicy} 处理；</li>
 *   <li>unacked：已投递未确认消息及其确认截止时间，超时后重投；</li>
 *   <li>lastAckedSeq：累积确认位点，断线续传从该位点之后继续；</li>
 *   <li>consecutiveDrops：连续丢弃计数，DROP_OLDEST 策略下超限则订阅被摘除。</li>
 * </ul>
 *
 * <p>所有方法以订阅实例为锁串行化，并发发布/拉取/确认不会串位；
 * 拉取等待只阻塞在自身监视器上，不影响其他会话与订阅。
 */
final class Subscription {

    /** 入队结果：兼容旧版 DropEvent，同时携带新版 BusEvent 列表。 */
    record EnqueueResult(DropEvent dropEvent, List<BusEvent> events) {
        static final EnqueueResult NONE = new EnqueueResult(null, List.of());
    }

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
    final BackpressurePolicy policy;

    private final Deque<Message> pending = new ArrayDeque<>();
    private final Map<Long, InFlight> unacked = new LinkedHashMap<>();
    private long lastAckedSeq = 0L;
    private int consecutiveDrops = 0;
    private boolean evicted = false;

    Subscription(String sessionId, String topic, BackpressurePolicy policy) {
        this.sessionId = sessionId;
        this.topic = topic;
        this.policy = policy;
    }

    synchronized boolean isEvicted() {
        return evicted;
    }

    synchronized long lastAckedSeq() {
        return lastAckedSeq;
    }

    synchronized int pendingCount() {
        return pending.size();
    }

    /**
     * 入队新消息。{@code sessionRoom} 为会话级剩余容量（会话总上限减去当前各订阅积压之和），
     * 实际可用空间取单订阅剩余与会话剩余的最小值；没有空间时按订阅背压策略处理。
     */
    synchronized EnqueueResult enqueue(Message message, BusConfig config, int sessionRoom) {
        if (evicted || message.seq() <= lastAckedSeq) {
            return EnqueueResult.NONE;
        }
        int room = Math.min(config.maxBacklogPerSubscription() - pending.size(), sessionRoom);
        if (room > 0) {
            pending.addLast(message);
            notifyAll();
            return EnqueueResult.NONE;
        }
        return overflow(message, config);
    }

    /**
     * 积压/会话总量超限：按策略丢弃最旧或摘除订阅，并生成可区分事件。
     *
     * <p>会话级总上限被其他订阅占满、本订阅积压为空时没有最旧可丢，
     * 此时丢弃触发超限的新消息本身：容量到顶属于正常背压，入队必须正常返回，
     * 不能以异常打断发布，也不能影响同一条消息对其他会话的投递。
     */
    private EnqueueResult overflow(Message message, BusConfig config) {
        List<BusEvent> events = new ArrayList<>();
        if (policy == BackpressurePolicy.EVICT_SUBSCRIPTION) {
            evicted = true;
            discardPendingAsGap(events);
            events.add(BusEvent.gap(sessionId, topic, message.seq(), message.seq(),
                    "背压策略 EVICT_SUBSCRIPTION：触发超限的消息被丢弃"));
            events.add(BusEvent.subscriptionEvicted(sessionId, topic,
                    "背压策略 EVICT_SUBSCRIPTION：订阅被摘除，会话保留，重连后可续传"));
            return new EnqueueResult(new DropEvent(sessionId, topic, message.seq(),
                    DropReason.SLOW_CONSUMER_EVICTED,
                    "背压策略 EVICT_SUBSCRIPTION：订阅被摘除"), events);
        }
        Message dropped = pending.pollFirst();
        boolean droppedIncoming = dropped == null;
        if (droppedIncoming) {
            // 会话总上限被其他订阅占满、本订阅积压为空：丢弃新消息本身
            dropped = message;
        }
        consecutiveDrops++;
        events.add(BusEvent.gap(sessionId, topic, dropped.seq(), dropped.seq(),
                droppedIncoming
                        ? "会话总积压达到上限，本订阅无最旧可丢，丢弃新到消息"
                        : "积压达到上限，丢弃最旧消息"));
        if (consecutiveDrops >= config.maxDropsBeforeDisconnect()) {
            evicted = true;
            discardPendingAsGap(events);
            if (!droppedIncoming) {
                // 新消息尚未入队即随摘除一并丢弃，缺口事件需覆盖其序号
                events.add(BusEvent.gap(sessionId, topic, message.seq(), message.seq(),
                        "订阅被摘除，新消息被丢弃"));
            }
            events.add(BusEvent.subscriptionEvicted(sessionId, topic,
                    "连续丢弃 " + consecutiveDrops + " 条，订阅被摘除，会话保留，重连后可续传"));
            return new EnqueueResult(new DropEvent(sessionId, topic, dropped.seq(),
                    DropReason.SLOW_CONSUMER_EVICTED,
                    "连续丢弃 " + consecutiveDrops + " 条，订阅被断开"), events);
        }
        if (!droppedIncoming) {
            pending.addLast(message);
            notifyAll();
        }
        return new EnqueueResult(new DropEvent(sessionId, topic, dropped.seq(),
                DropReason.BACKLOG_OVERFLOW,
                droppedIncoming
                        ? "会话总积压达到上限，本订阅无最旧可丢，丢弃新到消息 " + message.seq()
                        : "积压达到上限 " + config.maxBacklogPerSubscription() + "，丢弃最旧消息"), events);
    }

    /** 摘除订阅时把仍积压的消息作为一段连续缺口丢弃。 */
    private void discardPendingAsGap(List<BusEvent> events) {
        if (!pending.isEmpty()) {
            events.add(BusEvent.gap(sessionId, topic,
                    pending.peekFirst().seq(), pending.peekLast().seq(),
                    "订阅被摘除，积压消息被丢弃"));
            pending.clear();
        }
    }

    /** 收集已超时未确认、需要重投的消息（按序号升序）。 */
    synchronized List<Message> dueRedeliveries(long nowMillis) {
        List<Message> due = new ArrayList<>();
        for (InFlight inFlight : unacked.values()) {
            if (inFlight.deadlineMillis <= nowMillis) {
                due.add(inFlight.message);
            }
        }
        return due;
    }

    /** 取出最多 max 条待投递新消息（按序）。 */
    synchronized List<Message> takePending(int max) {
        List<Message> out = new ArrayList<>(Math.min(max, pending.size()));
        while (!pending.isEmpty() && out.size() < max) {
            out.add(pending.pollFirst());
        }
        return out;
    }

    /** 记录一次投递：首次投递建立跟踪，重投刷新截止时间与次数。 */
    synchronized void markDelivered(Message message, long nowMillis, long ackTimeoutMillis) {
        InFlight inFlight = unacked.get(message.seq());
        if (inFlight == null) {
            unacked.put(message.seq(), new InFlight(message, nowMillis + ackTimeoutMillis));
        } else {
            inFlight.attempt++;
            inFlight.deadlineMillis = nowMillis + ackTimeoutMillis;
        }
    }

    /** 当前投递次数（用于构造 Delivery.attempt）。 */
    synchronized int attemptOf(long seq) {
        InFlight inFlight = unacked.get(seq);
        return inFlight == null ? 0 : inFlight.attempt;
    }

    /** 累积确认：位点推进到 seq（含），清除对应未确认跟踪。 */
    synchronized void ackUpTo(long seq) {
        if (seq > lastAckedSeq) {
            lastAckedSeq = seq;
        }
        unacked.keySet().removeIf(s -> s <= seq);
    }

    /** 订阅恢复投递（如重连后），清零连续丢弃计数。 */
    synchronized void resetDrops() {
        consecutiveDrops = 0;
    }

    /** 断连时将所有未确认投递置为立即到期，重连后优先重投。 */
    synchronized void requeueInFlight() {
        for (InFlight inFlight : unacked.values()) {
            inFlight.deadlineMillis = 0L;
        }
    }

    /** 重连复活：解除摘除状态、清零丢弃计数、未确认消息置为待重投。 */
    synchronized void revive() {
        evicted = false;
        consecutiveDrops = 0;
        requeueInFlight();
        notifyAll();
    }

    /** 是否有可投递内容（重投到期或有新消息）。 */
    synchronized boolean hasDeliverable(long nowMillis) {
        if (evicted) {
            return false;
        }
        if (!pending.isEmpty()) {
            return true;
        }
        for (InFlight inFlight : unacked.values()) {
            if (inFlight.deadlineMillis <= nowMillis) {
                return true;
            }
        }
        return false;
    }

    /** 在自身监视器上有界等待，只阻塞调用方线程。 */
    synchronized void await(long millis) throws InterruptedException {
        if (millis > 0) {
            wait(millis);
        }
    }

    /** 唤醒正在等待的拉取（断线/关闭时调用），让等待方能立刻复查会话状态。 */
    synchronized void wakeup() {
        notifyAll();
    }
}
