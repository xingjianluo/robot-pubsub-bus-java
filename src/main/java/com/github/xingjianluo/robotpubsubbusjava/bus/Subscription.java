package com.github.xingjianluo.robotpubsubbusjava.bus;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;

/**
 * 单个订阅的运行时状态。
 *
 * <p><b>线程模型</b>：本对象不自带锁，所有方法都必须在所属会话的 gate 锁下调用
 * （见 {@code MessageBus}）。这样会话级缓冲总量可以在同一把锁内跨订阅核算，
 * 不同会话之间互不阻塞。
 *
 * <ul>
 *   <li>pending：以主题序号为键的有序积压（{@link TreeMap}）。多生产者并发发布时，
 *       消息到达订阅的次序可能与其主题序号不一致，按 seq 重排保证“会话内有序”；</li>
 *   <li>unacked：已投递未确认消息及确认截止时间，超时重投；</li>
 *   <li>lastAckedSeq：累积确认位点，断线续传 / 摘除重挂都从该位点之后继续；</li>
 *   <li>背压策略 {@link BackpressureStrategy} 按订阅选择：
 *       丢弃最旧（连续丢弃达阈值后摘除）或直接摘除整个订阅；</li>
 *   <li>gapReportedThrough / highWaterSeq：跟踪已向客户端报过的缺口下沿，
 *       以及摘除期间到达的最大序号，重挂时把缺失区间一次性补齐上报。</li>
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

    /** 入队产生的内部事件，由 MessageBus 分别翻译为新旧两类对外事件。 */
    record Occurrence(Kind kind, String topic, long seq, List<SeqGap> gaps, String detail) {
        enum Kind { OVERFLOW, EVICT_THRESHOLD, EVICT_STRATEGY }

        static Occurrence overflow(String topic, long seq, int limit) {
            return new Occurrence(Kind.OVERFLOW, topic, seq, List.of(new SeqGap(seq, seq)),
                    "订阅积压达到上限 " + limit + "，丢弃最旧消息 seq=" + seq);
        }

        static Occurrence sessionValve(String topic, long seq, int sessionLimit) {
            return new Occurrence(Kind.OVERFLOW, topic, seq, List.of(new SeqGap(seq, seq)),
                    "会话总缓冲达到上限 " + sessionLimit + "，丢弃会话内最旧消息 seq=" + seq);
        }
    }

    /** 一次入队的结果：可能为空（正常入队）或携带若干发生事件。 */
    static final class EnqueueOutcome {
        final List<Occurrence> occurrences = new ArrayList<>();
    }

    final String sessionId;
    final String topic;
    final BackpressureStrategy strategy;

    /** 待投递积压：按主题序号排序（多生产者并发时可能含空洞，如 64 已到而 63 未到）。 */
    private final TreeMap<Long, Message> pending = new TreeMap<>();
    private final TreeMap<Long, InFlight> unacked = new TreeMap<>();
    private long lastAckedSeq = 0L;
    /** 已 offer 到本订阅的最大主题序号（摘除期间用于补报缺口）。 */
    private long highWaterSeq = 0L;
    /**
     * 积压连续前沿：从当前最小积压序号起、无空洞地连续到达的最高序号。
     * 只有不超过该水位的积压才允许首次投递，确保乱序到达时更小号先按住。
     */
    private long contiguousHigh = 0L;
    private int consecutiveDrops = 0;
    private boolean evicted = false;
    /** 已通过事件上报过的最大序号（缺口下沿），初始与确认位点一致。 */
    private long gapReportedThrough = 0L;

    Subscription(String sessionId, String topic, BackpressureStrategy strategy) {
        this.sessionId = sessionId;
        this.topic = topic;
        this.strategy = strategy;
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

    /** 本订阅当前占用的会话缓冲：待投递 + 已投递未确认。 */
    int bufferedCount() {
        return pending.size() + unacked.size();
    }

    /** 会话总量阀：摘除本订阅最旧的一条积压；空队返回 null。 */
    Occurrence dropOldestForSessionValve(int sessionLimit) {
        var oldest = pending.pollFirstEntry();
        if (oldest == null) {
            return null;
        }
        long seq = oldest.getKey();
        // 丢弃产生显式缺口，连续前沿以新队首之前为基准
        contiguousHigh = pending.isEmpty() ? seq : pending.firstKey() - 1;
        advanceContiguous();
        markGapReported(seq);
        return Occurrence.sessionValve(topic, seq, sessionLimit);
    }

    /** 本订阅当前最旧待投递序号；空队返回 Long.MAX_VALUE。 */
    long oldestPendingSeq() {
        return pending.isEmpty() ? Long.MAX_VALUE : pending.firstKey();
    }

    /**
     * 入队新消息（调用方已保证会话总量有余量）。
     * 积压满时按订阅策略处理并返回事件；摘除期间的消息直接拒绝并跟踪序号。
     */
    EnqueueOutcome enqueue(Message message, BusConfig config) {
        EnqueueOutcome outcome = new EnqueueOutcome();
        highWaterSeq = Math.max(highWaterSeq, message.seq());
        if (evicted || message.seq() <= lastAckedSeq) {
            return outcome; // 摘除期间不缓冲；已确认位点之前的消息不再投递
        }
        if (pending.size() < config.maxBacklogPerSubscription()) {
            pending.put(message.seq(), message);
            advanceContiguous();
            return outcome;
        }
        if (strategy == BackpressureStrategy.EVICT_SUBSCRIPTION) {
            evictNow(Occurrence.Kind.EVICT_STRATEGY, message, config, outcome);
            return outcome;
        }
        // DROP_OLDEST：按序号丢弃积压中最旧的一条（丢弃产生显式缺口，可安全跳过）
        var dropped = pending.pollFirstEntry();
        consecutiveDrops++;
        if (consecutiveDrops >= config.maxDropsBeforeDisconnect()) {
            evictNow(Occurrence.Kind.EVICT_THRESHOLD, message, config, outcome, dropped.getKey());
            return outcome;
        }
        pending.put(message.seq(), message);
        // 丢弃产生显式缺口，连续前沿以新队首之前为基准重新推进
        contiguousHigh = pending.firstKey() - 1;
        advanceContiguous();
        markGapReported(dropped.getKey());
        outcome.occurrences.add(Occurrence.overflow(topic, dropped.getKey(),
                config.maxBacklogPerSubscription()));
        return outcome;
    }

    /**
     * 以当前积压最小序号为起点，沿“连续无空洞”的已到达序号把 {@link #contiguousHigh}
     * 尽可能向后推进。乱序到达的更大序号在更小号补齐前不会越过空洞。
     */
    private void advanceContiguous() {
        if (pending.isEmpty()) {
            return;
        }
        long cursor = Math.max(contiguousHigh, pending.firstKey() - 1);
        while (pending.containsKey(cursor + 1)) {
            cursor++;
        }
        contiguousHigh = cursor;
    }

    /** EVICT_SUBSCRIPTION 策略：积压满即摘除，作废的是当前积压与新来消息。 */
    private void evictNow(Occurrence.Kind kind, Message incoming, BusConfig config,
                          EnqueueOutcome outcome) {
        long[] lost = new long[pending.size() + 1];
        int i = 0;
        for (Long seq : pending.keySet()) {
            lost[i++] = seq;
        }
        lost[i] = incoming.seq();
        doEvict(kind, null, lost, incoming.seq(), outcome, config);
    }

    /** DROP_OLDEST 阈值摘除：刚丢了 droppedSeq，随后清掉剩余积压并拒绝新来消息。 */
    private void evictNow(Occurrence.Kind kind, Message incoming, BusConfig config,
                          EnqueueOutcome outcome, long droppedSeq) {
        long[] lost = new long[pending.size() + 2];
        int i = 0;
        lost[i++] = droppedSeq;
        for (Long seq : pending.keySet()) {
            lost[i++] = seq;
        }
        lost[i] = incoming.seq();
        doEvict(kind, droppedSeq, lost, incoming.seq(), outcome, config);
    }

    private void doEvict(Occurrence.Kind kind, Long thresholdSeq, long[] lostSeqs,
                         long incomingSeq, EnqueueOutcome outcome, BusConfig config) {
        List<SeqGap> gaps = coalesce(lostSeqs);
        evicted = true;
        pending.clear();
        contiguousHigh = 0L;
        consecutiveDrops = 0;
        markGapReported(incomingSeq);
        String detail = kind == Occurrence.Kind.EVICT_STRATEGY
                ? "订阅按 EVICT_SUBSCRIPTION 策略摘除，作废序号见 gaps"
                : "连续丢弃达到 " + config.maxDropsBeforeDisconnect()
                        + " 次，订阅被摘除（慢消费者）";
        outcome.occurrences.add(new Occurrence(kind, topic,
                thresholdSeq != null ? thresholdSeq : firstSeq(gaps), gaps, detail));
    }

    /**
     * 重连后重新挂上订阅：若摘除期间还有未上报的缺失序号，返回需要补报的缺口。
     * 非摘除状态仅清零连续丢弃计数，返回空。
     */
    List<SeqGap> rearm() {
        consecutiveDrops = 0;
        if (!evicted) {
            return List.of();
        }
        evicted = false;
        if (highWaterSeq > gapReportedThrough) {
            long from = gapReportedThrough + 1L;
            long to = highWaterSeq;
            gapReportedThrough = highWaterSeq;
            return List.of(new SeqGap(from, to));
        }
        return List.of();
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

    /**
     * 取出最多 max 条待投递新消息。只取积压内“连续无空洞”的前缀：
     * 起点为积压最小序号，终点不超过 {@link #contiguousHigh}。
     * 多生产者并发乱序到达时，更小号未到则更大的号先按住，严格保证会话内有序。
     */
    List<Message> takePending(int max) {
        List<Message> out = new ArrayList<>(Math.min(Math.max(max, 0), pending.size()));
        if (pending.isEmpty()) {
            return out;
        }
        long expected = pending.firstKey();
        while (out.size() < max && expected <= contiguousHigh) {
            Message message = pending.remove(expected);
            if (message == null) {
                break; // 理论上不会发生（contiguousHigh 内必连续），防御性中止
            }
            out.add(message);
            expected++;
        }
        if (pending.isEmpty()) {
            contiguousHigh = 0L;
        }
        return out;
    }

    /** 记录一次投递：首次投递建立跟踪并推进在投上沿，重投刷新截止时间与次数。 */
    void markDelivered(Message message, long nowMillis, long ackTimeoutMillis) {
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
        if (seq > gapReportedThrough) {
            gapReportedThrough = seq;
        }
        unacked.keySet().removeIf(s -> s <= seq);
    }

    /** 断连时将所有未确认投递置为立即到期，重连后优先重投。 */
    void requeueInFlight() {
        for (InFlight inFlight : unacked.values()) {
            inFlight.deadlineMillis = 0L;
        }
    }

    private void markGapReported(long seq) {
        if (seq > gapReportedThrough) {
            gapReportedThrough = seq;
        }
    }

    private static long firstSeq(List<SeqGap> gaps) {
        return gaps.isEmpty() ? 0L : gaps.get(0).fromSeq();
    }

    /** 把若干实际丢失的序号合并成连续区间。 */
    private static List<SeqGap> coalesce(long[] seqs) {
        if (seqs.length == 0) {
            return List.of();
        }
        long[] sorted = seqs.clone();
        Arrays.sort(sorted);
        List<SeqGap> gaps = new ArrayList<>();
        long from = sorted[0];
        long prev = sorted[0];
        for (int i = 1; i < sorted.length; i++) {
            long s = sorted[i];
            if (s == prev + 1L) {
                prev = s;
            } else {
                gaps.add(new SeqGap(from, prev));
                from = s;
                prev = s;
            }
        }
        gaps.add(new SeqGap(from, prev));
        return gaps;
    }
}
