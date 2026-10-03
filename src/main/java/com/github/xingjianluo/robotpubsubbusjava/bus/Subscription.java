package com.github.xingjianluo.robotpubsubbusjava.bus;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;

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
        // sessionRoom 为 0 而本订阅尚有空间：容量是被同会话其他主题的积压占满的
        boolean sessionCapacityExhausted = sessionRoom <= 0
                && pending.size() < config.maxBacklogPerSubscription();
        return overflow(message, config, sessionCapacityExhausted);
    }

    /**
     * 积压/会话总量超限：按策略丢弃最旧或摘除订阅，并生成可区分事件。
     *
     * <p>{@code sessionCapacityExhausted} 表示超限来自会话总上限而非本订阅积压：
     * 此时本订阅的 pending 可能为空（容量被其他主题占满），DROP_OLDEST 不能去
     * poll 空队列（否则 NPE），而是把触发超限的新消息自身记为一条缺口——
     * 它对本订阅确实未投递，序号缺口必须完整对得上。
     */
    private EnqueueResult overflow(Message message, BusConfig config,
                                   boolean sessionCapacityExhausted) {
        List<BusEvent> events = new ArrayList<>();
        String dropContext = sessionCapacityExhausted
                ? "会话总积压达到上限（容量被其他主题占用）"
                : "积压达到上限";
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
        consecutiveDrops++;
        if (dropped != null) {
            events.add(BusEvent.gap(sessionId, topic, dropped.seq(), dropped.seq(),
                    dropContext + "，丢弃最旧消息"));
        } else {
            // 本订阅无积压可丢（容量被同会话其他主题占满）：新消息自身成为缺口
            events.add(BusEvent.gap(sessionId, topic, message.seq(), message.seq(),
                    dropContext + "，本订阅无积压可丢，新消息被丢弃"));
        }
        if (consecutiveDrops >= config.maxDropsBeforeDisconnect()) {
            evicted = true;
            discardPendingAsGap(events);
            if (dropped != null) {
                // 常规丢弃只记了被丢的旧消息；新消息因订阅摘除同样无法入队，需补一条缺口
                events.add(BusEvent.gap(sessionId, topic, message.seq(), message.seq(),
                        "订阅被摘除，新消息被丢弃"));
            }
            // dropped == null 时新消息的缺口已在上方记录，不重复记
            events.add(BusEvent.subscriptionEvicted(sessionId, topic,
                    "连续丢弃 " + consecutiveDrops + " 条，订阅被摘除，会话保留，重连后可续传"));
            return new EnqueueResult(new DropEvent(sessionId, topic,
                    dropped != null ? dropped.seq() : message.seq(),
                    DropReason.SLOW_CONSUMER_EVICTED,
                    "连续丢弃 " + consecutiveDrops + " 条，订阅被断开"), events);
        }
        if (dropped != null) {
            pending.addLast(message);
            notifyAll();
        }
        // dropped == null：新消息已作为缺口丢弃，不入队，会话总占用保持不变
        return new EnqueueResult(new DropEvent(sessionId, topic,
                dropped != null ? dropped.seq() : message.seq(),
                DropReason.BACKLOG_OVERFLOW,
                dropContext + "，按背压策略丢弃"), events);
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

    /** 断连时在订阅监视器内置未确认消息为立即到期并唤醒等待中的拉取，与拉取的“检查→等待”互斥。 */
    synchronized void markDisconnected() {
        for (InFlight inFlight : unacked.values()) {
            inFlight.deadlineMillis = 0L;
        }
        notifyAll();
    }

    /** 重连复活：解除摘除状态、清零丢弃计数、未确认消息置为待重投。 */
    synchronized void revive() {
        evicted = false;
        consecutiveDrops = 0;
        for (InFlight inFlight : unacked.values()) {
            inFlight.deadlineMillis = 0L;
        }
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

    /**
     * 在自身监视器上有界等待；仅当 {@code waitingAllowed} 仍为 true 时才真正进入等待。
     * 条件判断与 wait 在同一 synchronized 临界区，配合 {@link #markDisconnected()} 的
     * notifyAll 构成标准守卫块：断线若发生在判断之前，本次不再等待；发生在等待期间，
     * 等待被立即唤醒，由调用方在循环顶部复查连接状态后拒绝投递。
     */
    synchronized void awaitWhileWaitingAllowed(long millis, BooleanSupplier waitingAllowed)
            throws InterruptedException {
        if (millis > 0 && waitingAllowed.getAsBoolean()) {
            wait(millis);
        }
    }
}
