package com.github.xingjianluo.robotpubsubbusjava.bus.engine;

import com.github.xingjianluo.robotpubsubbusjava.bus.api.BusSession;
import com.github.xingjianluo.robotpubsubbusjava.bus.api.MessageBus;
import com.github.xingjianluo.robotpubsubbusjava.bus.auth.AuthorizationService;
import com.github.xingjianluo.robotpubsubbusjava.bus.config.BusProperties;
import com.github.xingjianluo.robotpubsubbusjava.bus.error.BusErrorCode;
import com.github.xingjianluo.robotpubsubbusjava.bus.error.BusException;
import com.github.xingjianluo.robotpubsubbusjava.bus.model.BackpressurePolicy;
import com.github.xingjianluo.robotpubsubbusjava.bus.model.DeliveryAttempt;
import com.github.xingjianluo.robotpubsubbusjava.bus.model.Message;
import com.github.xingjianluo.robotpubsubbusjava.bus.model.Principal;
import com.github.xingjianluo.robotpubsubbusjava.bus.model.PublishResult;
import com.github.xingjianluo.robotpubsubbusjava.bus.model.ResumeToken;
import com.github.xingjianluo.robotpubsubbusjava.bus.model.SessionEvent;
import com.github.xingjianluo.robotpubsubbusjava.bus.model.StartPosition;
import com.github.xingjianluo.robotpubsubbusjava.bus.model.SubscribeResult;
import com.github.xingjianluo.robotpubsubbusjava.bus.model.TopicConfig;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 进程内本地总线实现。
 *
 * <p>投递语义：主题内全局有序；每订阅会话独立位点；至少一次投递；
 * ACK 推进已确认位点（cumulative ack），已确认消息不重复交付；inflight 消息在确认超时后
 * 退回 pending 重投（投递事件标记 RETRY 且 attemptCount 递增）。
 *
 * <p>背压：单订阅者积压（pending+inflight）达到主题 maxInflight 即触发策略。
 * REJECT 在任何投递前统一裁决（全部订阅者均无空位才拒绝），保证要么全收要么全拒；
 * DROP_OLDEST 丢弃该订阅者最旧未确认消息并下发 Dropped 通知；
 * DISCONNECT 断开慢订阅者并下发 SessionClosed。判定对活动/休眠订阅一致，无无界增长。
 */
public class LocalMessageBus implements MessageBus {

    private final BusProperties properties;
    private final AuthorizationService auth;
    private final Map<String, TopicState> topics = new ConcurrentHashMap<>();
    private final Map<String, SessionState> sessions = new ConcurrentHashMap<>();
    private final ScheduledExecutorService reaper;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicInteger sessionSeq = new AtomicInteger();

    public LocalMessageBus(BusProperties properties, AuthorizationService authorizationService) {
        this.properties = properties;
        this.auth = authorizationService;
        buildTopics();
        this.reaper = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "bus-ack-reaper");
            t.setDaemon(true);
            return t;
        });
        this.reaper.scheduleAtFixedRate(this::reapAll,
                properties.getReaperIntervalMillis(),
                properties.getReaperIntervalMillis(),
                TimeUnit.MILLISECONDS);
    }

    private void buildTopics() {
        properties.getTopics().forEach((name, props) -> {
            BackpressurePolicy policy = BackpressurePolicy.valueOf(props.getPolicy().trim().toUpperCase());
            topics.put(name, new TopicState(new TopicConfig(
                    name, policy, props.getMaxInflight(), props.getRetentionSize(),
                    props.getMaxPayloadBytes())));
        });
    }

    private void checkOpen() {
        if (closed.get()) {
            throw new BusException(BusErrorCode.BUS_CLOSED, "消息总线已关闭");
        }
    }

    // ------------------------------------------------------------------ 会话

    @Override
    public BusSession connect(Principal principal, String resumeTokenRaw) {
        checkOpen();
        Objects.requireNonNull(principal, "principal");
        if (resumeTokenRaw == null || resumeTokenRaw.isBlank()) {
            SessionState state = new SessionState(principal);
            sessions.put(state.sessionId, state);
            return state.newHandle();
        }
        ResumeToken token = ResumeToken.decode(resumeTokenRaw)
                .orElseThrow(() -> new BusException(BusErrorCode.RESUME_FORBIDDEN,
                        "续传令牌畸形，无法解析"));
        if (!principal.name().equals(token.principalName())) {
            throw new BusException(BusErrorCode.RESUME_FORBIDDEN,
                    "续传令牌属于身份 '" + token.principalName() + "'，与会话身份 '"
                            + principal.name() + "' 不匹配");
        }
        SessionState state = sessions.get(token.sessionId());
        if (state == null) {
            throw new BusException(BusErrorCode.RESUME_FORBIDDEN,
                    "续传令牌引用的会话不存在: " + token.sessionId());
        }
        if (state.live.get()) {
            throw new BusException(BusErrorCode.SESSION_CONFLICT,
                    "会话 " + token.sessionId() + " 仍有活动连接，拒绝重复接管");
        }
        if (!state.principal.name().equals(principal.name())) {
            throw new BusException(BusErrorCode.RESUME_FORBIDDEN, "会话身份不一致");
        }
        // 主动断线与慢消费断开均保留订阅与位点，可续传；terminated 仅用于总线关闭等终结场景
        return state.resume();
    }

    /** 传输层按 ID 取会话句柄（校验身份与连接代际，防止越权操作他人会话）。 */
    public BusSession sessionHandle(String sessionId, Principal principal) {
        checkOpen();
        SessionState state = sessions.get(sessionId);
        if (state == null || state.terminated) {
            throw new BusException(BusErrorCode.SESSION_CLOSED, "会话不存在或已关闭: " + sessionId);
        }
        if (!state.principal.name().equals(principal.name())) {
            throw new BusException(BusErrorCode.RESUME_FORBIDDEN,
                    "会话 " + sessionId + " 不属于身份 " + principal.name());
        }
        if (!state.live.get()) {
            throw new BusException(BusErrorCode.SESSION_CLOSED,
                    "会话 " + sessionId + " 已断开，需持续传令牌重连");
        }
        return state.newHandle();
    }

    // ------------------------------------------------------------------ 发布

    @Override
    public PublishResult publish(Principal principal, String topicName, byte[] payload) {
        checkOpen();
        Objects.requireNonNull(principal, "principal");
        if (payload == null) {
            throw new BusException(BusErrorCode.MALFORMED_MESSAGE, "报文负载为 null（畸形报文）");
        }
        TopicState topic = topics.get(topicName);
        if (topic == null) {
            throw new BusException(BusErrorCode.UNKNOWN_TOPIC, "未知主题: " + topicName);
        }
        if (!auth.canPublish(principal, topicName)) {
            throw new BusException(BusErrorCode.PUBLISH_FORBIDDEN,
                    "身份 '" + principal.name() + "' 无权向主题 '" + topicName + "' 发布");
        }
        if (payload.length > topic.config.maxPayloadBytes()) {
            throw new BusException(BusErrorCode.PAYLOAD_TOO_LARGE,
                    "报文 " + payload.length + " 字节，超过主题 '" + topicName + "' 上限 "
                            + topic.config.maxPayloadBytes() + " 字节");
        }
        Message message;
        List<SubscriptionState> subscribers;
        synchronized (topic.lock) {
            long seq = ++topic.lastSeq;
            message = new Message(topicName, seq, principal.name(), payload.clone());
            topic.retained.put(seq, message);
            while (topic.retained.size() > topic.config.retentionSize()) {
                topic.retained.pollFirstEntry();
            }
            subscribers = new ArrayList<>(topic.subscribers.values());

            // REJECT 需在任何投递之前统一裁决：存在一个无空位订阅者即整笔拒绝（消息不扇出）
            if (topic.config.backpressurePolicy() == BackpressurePolicy.REJECT) {
                for (SubscriptionState sub : subscribers) {
                    if (sub.backlog() >= topic.config.maxInflight()) {
                        throw new BusException(BusErrorCode.SUBSCRIBER_BACKPRESSURE_REJECTED,
                                "订阅者 " + sub.session.sessionId + "/" + sub.subscriptionId
                                        + " 积压已达上限 " + topic.config.maxInflight()
                                        + "，按 REJECT 策略拒绝发布；消息 seq=" + seq + " 未入任何订阅队列");
                    }
                }
            }
            for (SubscriptionState sub : subscribers) {
                sub.onPublished(message, topic.config);
            }
        }
        // 事件入队与泵取放在主题锁外：慢消费者只阻塞其自身的投递循环，不影响其他订阅者
        for (SubscriptionState sub : subscribers) {
            sub.deliverPending();
        }
        return new PublishResult(topicName, message.seq());
    }

    @Override
    public Collection<TopicConfig> topics() {
        checkOpen();
        return topics.values().stream().map(t -> t.config)
                .sorted(Comparator.comparing(TopicConfig::name)).toList();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            reaper.shutdownNow();
            for (SessionState s : sessions.values()) {
                s.terminate(BusErrorCode.BUS_CLOSED.name(), "总线关闭");
            }
        }
    }

    // ------------------------------------------------------------------ 重投

    private void reapAll() {
        try {
            for (TopicState topic : topics.values()) {
                long now = System.currentTimeMillis();
                List<SubscriptionState> snapshot;
                synchronized (topic.lock) {
                    snapshot = new ArrayList<>(topic.subscribers.values());
                }
                for (SubscriptionState sub : snapshot) {
                    boolean changed;
                    synchronized (topic.lock) {
                        changed = sub.reapTimedOut(now, properties.getAckTimeoutMillis());
                    }
                    if (changed) {
                        sub.deliverPending();
                    }
                }
            }
        } catch (RuntimeException ignored) {
            // 后台线程不应因单次异常死亡
        }
    }

    // ==================================================================
    // 主题状态
    // ==================================================================
    private final class TopicState {
        final TopicConfig config;
        final ReentrantLock lock = new ReentrantLock();
        final NavigableMap<Long, Message> retained = new TreeMap<>();
        final Map<String, SubscriptionState> subscribers = new HashMap<>();
        long lastSeq = 0;

        TopicState(TopicConfig config) {
            this.config = config;
        }
    }

    // ==================================================================
    // 订阅状态（全部可变状态仅在所属主题锁内访问，deliverPending 的事件入队除外）
    // ==================================================================
    private final class SubscriptionState {
        final String subscriptionId;
        final SessionState session;
        final TopicState topic;
        final TreeSet<Long> pending = new TreeSet<>();
        final TreeMap<Long, Long> inflightSince = new TreeMap<>(); // seq -> 本次投递时间
        final TreeMap<Long, Integer> attempts = new TreeMap<>();  // seq -> 累计投递次数
        long committed = 0;                                       // 累计确认位点（<= 均已确认）
        /** DISCONNECT 策略生效后该订阅被摘除（休眠）；重连时重新注册并续传。 */
        volatile boolean evicted = false;

        SubscriptionState(SessionState session, TopicState topic, String subscriptionId) {
            this.session = session;
            this.topic = topic;
            this.subscriptionId = subscriptionId;
        }

        int backlog() {
            return pending.size() + inflightSince.size();
        }

        /** 主题锁内：新消息扇出给本订阅。 */
        void onPublished(Message message, TopicConfig config) {
            long seq = message.seq();
            if (seq <= committed || session.terminated || evicted) {
                // 已摘除的慢订阅休眠期不参与扇出（不积累新消息），重新订阅时从保留窗口续传
                return;
            }
            boolean hasRoom = backlog() < config.maxInflight();
            if (hasRoom) {
                pending.add(seq);
                return;
            }
            // 积压达上限：执行确定策略，杜绝无界增长。
            // REJECT 已在 LocalMessageBus.publish 扇出前统一裁决并抛错，故不会进入此处。
            switch (config.backpressurePolicy()) {
                case REJECT -> throw new IllegalStateException(
                        "REJECT 策略应在发布扇出前被拒绝，不应进入订阅扇出: " + subscriptionId);
                case DROP_OLDEST -> dropOldestThenEnqueue(seq, config);
                case DISCONNECT -> {
                    // 触发摘除的本条消息不扇出给休眠订阅；其仍进入主题保留窗口，
                    // 重新订阅/重连后可按保留窗口与位点追回
                    evict("积压达上限 " + config.maxInflight()
                            + "，按 DISCONNECT 策略摘除慢订阅；重新订阅/重连后从位点 "
                            + committed + " 续传");
                }
            }
        }

        /** 主题锁内：摘除慢订阅（订阅级隔离，不影响同会话其他订阅与发布能力）。 */
        private void evict(String message) {
            if (evicted) {
                return;
            }
            evicted = true;
            topic.subscribers.remove(subscriptionId);
            returnInflightToPending();
            session.offerControl(new SessionEvent.SessionClosed(subscriptionId,
                    BusErrorCode.SLOW_CONSUMER_DISCONNECTED.name(), message));
        }

        /** DROP_OLDEST：丢弃全局最旧未确认消息（在途优先，其次待发队首），顺移窗口并放入新序号。 */
        private void dropOldestThenEnqueue(long newSeq, TopicConfig config) {
            long dropped;
            long inflightHead = inflightSince.isEmpty() ? Long.MAX_VALUE : inflightSince.firstKey();
            long pendingHead = pending.isEmpty() ? Long.MAX_VALUE : pending.first();
            if (inflightHead <= pendingHead) {
                dropped = inflightHead;
                inflightSince.remove(dropped);
            } else {
                dropped = pending.pollFirst();
            }
            attempts.remove(dropped);
            // 丢弃即视为该序号及其之前已不可交付：顺移确认位点，窗口释放且永不重投
            if (dropped > committed) {
                committed = dropped;
                pending.headSet(committed, true).clear();
                inflightSince.headMap(committed, true).clear();
                attempts.headMap(committed, true).clear();
            }
            session.offerControl(new SessionEvent.Dropped(subscriptionId, dropped, dropped,
                    "积压达上限 " + config.maxInflight()
                            + "，按 DROP_OLDEST 策略丢弃最旧未确认消息，新消息 seq=" + newSeq
                            + "，确认位点顺移至 " + committed));
            if (newSeq > committed) {
                pending.add(newSeq);
            }
        }

        /** 主题锁内：把超时的单条在途消息退回待发队首，返回是否有变化。 */
        boolean reapTimedOut(long now, long ackTimeoutMillis) {
            boolean changed = false;
            var it = inflightSince.entrySet().iterator();
            while (it.hasNext()) {
                var e = it.next();
                if (now - e.getValue() >= ackTimeoutMillis) {
                    it.remove();
                    pending.add(e.getKey());
                    changed = true;
                }
            }
            return changed;
        }

        /** 主题锁内：断开时在途消息退回待发，等待重连后重投。 */
        void returnInflightToPending() {
            pending.addAll(inflightSince.keySet());
            inflightSince.clear();
        }

        /** 主题锁内：累计 ACK，<= seq 一律视为已确认并推进位点、清理状态。 */
        void applyAck(long seq) {
            if (seq <= committed) {
                return;
            }
            committed = seq;
            pending.headSet(committed, true).clear();
            inflightSince.headMap(committed, true).clear();
            attempts.headMap(committed, true).clear();
        }

        /**
         * 主题锁内决定下一条待投递消息并占位，锁外做有界事件入队。
         *
         * <p>严格有序的单在途窗口：在途（inflight）至多 1 条；只要窗口空闲，
         * 就取待发集合的最小序号投递。被丢弃消息通过顺移 committed 出清待发前缀，
         * 故队首恒为下一条；超时消息退回待发后仍为队首并标记 RETRY。
         * 慢消费者只在本订阅循环内有限等待，不影响其他订阅者。
         */
        void deliverPending() {
            while (true) {
                long seq;
                Message message;
                DeliveryAttempt attemptKind;
                int attemptCount;
                synchronized (topic.lock) {
                    if (!session.live.get() || session.terminated || evicted) {
                        return;
                    }
                    if (!inflightSince.isEmpty() || pending.isEmpty()) {
                        return; // 单在途：已有消息在途时不得投递下一条
                    }
                    seq = pending.pollFirst();
                    if (seq <= committed) {
                        continue;
                    }
                    message = topic.retained.get(seq);
                    if (message == null) {
                        // 保留窗口已覆盖请求位点：显式缺口通知而非静默跳过，并跳过窗口外序号
                        long oldest = topic.retained.isEmpty() ? -1 : topic.retained.firstKey();
                        session.offerControl(new SessionEvent.Gap(subscriptionId, seq, oldest));
                        committed = Math.max(committed,
                                topic.retained.isEmpty() ? seq : topic.retained.firstKey() - 1);
                        pending.headSet(committed, true).clear();
                        continue;
                    }
                    attemptCount = attempts.merge(seq, 1, Integer::sum);
                    attemptKind = attemptCount == 1 ? DeliveryAttempt.FIRST : DeliveryAttempt.RETRY;
                    inflightSince.put(seq, System.currentTimeMillis());
                }

                SessionEvent.Delivery event = new SessionEvent.Delivery(
                        subscriptionId, message, attemptKind, attemptCount);
                if (!session.offerDeliverable(event)) {
                    // 事件队列饱和（未及时 poll）：回滚占位并仅摘除本订阅，
                    // 原因码 SLOW_CONSUMER_DISCONNECTED 与积压上限触发一致，但与 REJECT/DROP 可区分
                    synchronized (topic.lock) {
                        inflightSince.remove(seq);
                        if (attemptCount == 1) {
                            attempts.remove(seq);
                        }
                        pending.add(seq);
                        evict("会话事件队列在有界等待内未被消费，订阅 " + subscriptionId
                                + " 被判定为慢消费者并摘除，重新订阅后续传");
                    }
                    return;
                }
            }
        }

        /** 主题锁内：重新注册被摘除的订阅，按保留窗口与已确认位点重建待投递集合。 */
        void reactivateAfterEviction() {
            if (!evicted) {
                return;
            }
            evicted = false;
            topic.subscribers.put(subscriptionId, this);
            returnInflightToPending();
            // 休眠期不扇出新消息；重激活时以保留窗口为准：窗口内、位点之后的全部纳入重投，
            // 保留 attempts 以便正确区分 RETRY（曾投递）与 FIRST（从未投递）。
            if (!topic.retained.isEmpty() && topic.retained.firstKey() > committed + 1) {
                // 期望的下一条已被保留窗口覆盖：显式缺口通知，绝不静默跳过
                session.offerControl(new SessionEvent.Gap(subscriptionId,
                        committed + 1, topic.retained.firstKey()));
            }
            for (Message m : topic.retained.values()) {
                if (m.seq() > committed) {
                    pending.add(m.seq());
                }
            }
        }
    }

    // ==================================================================
    // 会话状态
    // ==================================================================
    final class SessionState {
        final String sessionId;
        final Principal principal;
        final Map<String, SubscriptionState> subscriptions = new HashMap<>();
        final LinkedBlockingQueue<SessionEvent> events;
        final AtomicBoolean live = new AtomicBoolean(true);
        final AtomicInteger generation = new AtomicInteger(0);
        volatile boolean terminated = false;

        SessionState(Principal principal) {
            this.principal = principal;
            this.sessionId = "s-" + sessionSeq.incrementAndGet()
                    + "-" + Long.toHexString(System.nanoTime());
            // 事件队列有界：容量覆盖各主题最大积压之和，杜绝无界内存增长
            int cap = topics.values().stream().mapToInt(t -> t.config.maxInflight()).sum() + 32;
            this.events = new LinkedBlockingQueue<>(Math.max(cap, 64));
        }

        LocalBusSession newHandle() {
            return new LocalBusSession(this, generation.get());
        }

        /** 重连：代际 +1，重建在途/被摘除订阅，然后从已确认位点续传。 */
        LocalBusSession resume() {
            events.clear();
            for (SubscriptionState sub : subscriptions.values()) {
                synchronized (sub.topic.lock) {
                    if (sub.evicted) {
                        sub.reactivateAfterEviction();
                    } else {
                        sub.returnInflightToPending();
                    }
                }
            }
            live.set(true);
            generation.incrementAndGet();
            for (SubscriptionState sub : new ArrayList<>(subscriptions.values())) {
                sub.deliverPending();
            }
            return new LocalBusSession(this, generation.get());
        }

        void disconnect(int gen) {
            if (generation.get() != gen || !live.compareAndSet(true, false)) {
                return;
            }
            for (SubscriptionState sub : subscriptions.values()) {
                synchronized (sub.topic.lock) {
                    if (!sub.evicted) {
                        sub.returnInflightToPending();
                    }
                }
            }
        }

        void terminate(String reasonCode, String message) {
            if (!terminated) {
                offerControl(new SessionEvent.SessionClosed("", reasonCode, message));
            }
            terminated = true;
            live.set(false);
            generation.incrementAndGet();
        }

        boolean offerControl(SessionEvent event) {
            return events.offer(event);
        }

        boolean offerDeliverable(SessionEvent.Delivery event) {
            try {
                // 有界等待：发布线程不会被慢消费者无限拖住
                return events.offer(event, 50, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
    }

    /**
     * 会话句柄：绑定连接代际；断线/被断开后旧句柄操作一律失败，必须重连取新句柄。
     */
    public final class LocalBusSession implements BusSession {
        private final SessionState state;
        private final int generation;

        LocalBusSession(SessionState state, int generation) {
            this.state = state;
            this.generation = generation;
        }

        private void checkLive() {
            if (closed.get() || state.terminated) {
                throw new BusException(BusErrorCode.SESSION_CLOSED, "会话已关闭: " + state.sessionId);
            }
            if (state.generation.get() != generation || !state.live.get()) {
                throw new BusException(BusErrorCode.SESSION_CLOSED,
                        "连接已断开（代际失效），请使用续传令牌重连: " + state.sessionId);
            }
        }

        @Override
        public String sessionId() {
            return state.sessionId;
        }

        @Override
        public Principal principal() {
            return state.principal;
        }

        @Override
        public ResumeToken resumeToken() {
            return new ResumeToken(state.sessionId, state.principal.name());
        }

        @Override
        public SubscribeResult subscribe(String topicName, StartPosition startPosition) {
            checkLive();
            TopicState topic = topics.get(topicName);
            if (topic == null) {
                throw new BusException(BusErrorCode.UNKNOWN_TOPIC, "未知主题: " + topicName);
            }
            if (!auth.canSubscribe(state.principal, topicName)) {
                throw new BusException(BusErrorCode.SUBSCRIBE_FORBIDDEN,
                        "身份 '" + state.principal.name() + "' 无权订阅主题 '" + topicName + "'");
            }
            SubscriptionState sub;
            boolean resumedEvicted = false;
            synchronized (topic.lock) {
                SubscriptionState existing = state.subscriptions.get(topicName);
                if (existing != null && !existing.evicted) {
                    return new SubscribeResult(existing.subscriptionId, existing.committed, false);
                }
                if (existing != null) {
                    // 慢消费被摘除的订阅：重新订阅即从已确认位点与保留窗口续传
                    existing.reactivateAfterEviction();
                    sub = existing;
                    resumedEvicted = true;
                } else {
                    String subId = state.sessionId + ":" + topicName;
                    sub = new SubscriptionState(state, topic, subId);
                    state.subscriptions.put(topicName, sub);
                    topic.subscribers.put(subId, sub);

                    StartPosition pos = startPosition == null ? StartPosition.EARLIEST : startPosition;
                    if (pos == StartPosition.LATEST) {
                        sub.committed = topic.lastSeq;
                    } else if (!topic.retained.isEmpty()) {
                        long earliest = topic.retained.firstKey();
                        if (earliest > 1) {
                            // EARLIEST 但保留窗口已覆盖历史：如实通知缺口，不静默跳过
                            state.offerControl(new SessionEvent.Gap(subId, 1, earliest));
                        }
                        for (Message m : topic.retained.values()) {
                            if (m.seq() > sub.committed) {
                                sub.pending.add(m.seq());
                            }
                        }
                    }
                }
            }
            sub.deliverPending();
            return new SubscribeResult(sub.subscriptionId, sub.committed, resumedEvicted);
        }

        @Override
        public void ack(String topicName, long seq) {
            checkLive();
            TopicState topic = topics.get(topicName);
            if (topic == null) {
                throw new BusException(BusErrorCode.UNKNOWN_TOPIC, "未知主题: " + topicName);
            }
            if (seq < 0) {
                throw new BusException(BusErrorCode.MALFORMED_MESSAGE, "ACK 序号不能为负: " + seq);
            }
            SubscriptionState sub;
            synchronized (topic.lock) {
                sub = state.subscriptions.get(topicName);
                if (sub == null) {
                    throw new BusException(BusErrorCode.SUBSCRIPTION_NOT_FOUND,
                            "会话未订阅主题 '" + topicName + "'");
                }
                if (sub.evicted) {
                    throw new BusException(BusErrorCode.SLOW_CONSUMER_DISCONNECTED,
                            "订阅 " + sub.subscriptionId + " 已因慢消费被摘除（位点="
                                    + sub.committed + "），请重新订阅后续传，再进行确认");
                }
                sub.applyAck(seq);
            }
            sub.deliverPending();
        }

        @Override
        public PublishResult publish(String topicName, byte[] payload) {
            checkLive();
            return LocalMessageBus.this.publish(state.principal, topicName, payload);
        }

        @Override
        public SessionEvent poll(long timeoutMillis) throws InterruptedException {
            checkLive();
            if (timeoutMillis <= 0) {
                return state.events.poll();
            }
            return state.events.poll(timeoutMillis, TimeUnit.MILLISECONDS);
        }

        @Override
        public void disconnect() {
            state.disconnect(generation);
        }

        @Override
        public boolean closed() {
            return state.terminated || state.generation.get() != generation || !state.live.get();
        }
    }
}
