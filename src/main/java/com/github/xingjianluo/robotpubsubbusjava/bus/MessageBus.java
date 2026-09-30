package com.github.xingjianluo.robotpubsubbusjava.bus;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 本地发布/订阅消息总线。
 *
 * <p>核心语义：
 * <ul>
 *   <li>会话内按主题序号有序投递，至少一次（at-least-once），首投与重投可区分；</li>
 *   <li>建会话时由 {@link ClientDirectory} 凭令牌核验身份，客户端不能自报身份；</li>
 *   <li>断线保留订阅状态与确认位点，断线期间发布/拉取被明确挡住，重连后续传；
 *       关闭则会话作废、不可重连；</li>
 *   <li>背压策略按订阅选择（丢弃最旧 / 摘除订阅），丢弃形成可识别序号缺口；
 *       订阅被摘除后会话保留，重连可从已确认位点续收；</li>
 *   <li>每个会话有跨订阅的总缓冲上限，慢会话只在自己的锁/条件变量上有上限地等待，
 *       不拖住其他会话与其他订阅；</li>
 *   <li>发布/订阅按服务端确认的身份做主题级授权，所有失败以 {@link ErrorCode}
 *       区分并显式抛出，不静默断开、不静默丢消息。</li>
 * </ul>
 *
 * <p>并发模型：每个会话一把 gate 锁，该会话所有订阅状态的读写、总量核算、
 * 投递等待都在锁内完成；不同会话完全并行。发布者扇出时只短暂获取各订阅者会话的锁。
 */
public class MessageBus {

    private static final class Session {
        final String id;
        final String token;
        final SessionIdentity identity;
        /** key: topic */
        final Map<String, Subscription> subscriptions = new ConcurrentHashMap<>();
        /** 新模型事件（缺口 / 摘除 / 关闭）。 */
        final List<SessionEvent> events = new ArrayList<>();
        /** 旧模型事件，兼容既有进程内用法与测试。 */
        final List<DropEvent> dropEvents = new ArrayList<>();
        /** 会话 gate 锁：保护本会话所有订阅状态与总量。 */
        final ReentrantLock gate = new ReentrantLock();
        /** 有待投递消息 / 状态变化时唤醒本会话等待者。 */
        final Condition available = gate.newCondition();
        volatile boolean connected = true;
        volatile boolean closed = false;

        Session(String id, String token, SessionIdentity identity) {
            this.id = id;
            this.token = token;
            this.identity = identity;
        }
    }

    private final BusConfig config;
    private final Clock clock;
    private final Set<String> topics = ConcurrentHashMap.newKeySet();
    private final Map<String, Session> sessions = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> topicSequences = new ConcurrentHashMap<>();
    /** 客户端凭据登记册；默认空（仅进程内直接 openSession 可用），Spring 装配时注入。 */
    private ClientDirectory clientDirectory = token -> java.util.Optional.empty();

    public MessageBus(BusConfig config, Clock clock) {
        this.config = config;
        this.clock = clock;
    }

    /** 注册主题；未注册主题的发布/订阅将以 UNKNOWN_TOPIC 拒绝。 */
    public void registerTopic(String topic) {
        topics.add(topic);
        topicSequences.putIfAbsent(topic, new AtomicLong(0));
    }

    /** 当前已注册主题清单（快照）。 */
    public List<String> listTopics() {
        return topics.stream().sorted().toList();
    }

    /**
     * 进程内直接开启会话（保留既有用法）：身份由调用方给出。
     * 网络入口必须使用 {@link #authenticate(String)}，由服务端核验令牌。
     */
    public String openSession(SessionIdentity identity) {
        if (identity == null) {
            throw new BusException(ErrorCode.UNAUTHENTICATED, "缺少会话身份");
        }
        String sessionId = identity.clientId() + "-" + UUID.randomUUID();
        sessions.put(sessionId, new Session(sessionId, null, identity));
        return sessionId;
    }

    /**
     * 凭客户端令牌建会话。令牌缺失为 UNAUTHENTICATED，令牌不合法为 INVALID_CREDENTIALS，
     * 会话身份完全由服务端登记材料推导。
     *
     * @return 会话标识与服务端认下的客户端标识
     */
    public SessionOpening authenticate(String token) {
        if (token == null || token.isBlank()) {
            throw new BusException(ErrorCode.UNAUTHENTICATED, "请求未携带客户端令牌");
        }
        RegisteredClient client = clientDirectory.resolve(token)
                .orElseThrow(() -> new BusException(ErrorCode.INVALID_CREDENTIALS,
                        "客户端令牌无效或已注销"));
        String sessionId = client.clientId() + "-" + UUID.randomUUID();
        sessions.put(sessionId, new Session(sessionId, token, client.toIdentity()));
        return new SessionOpening(sessionId, client.clientId());
    }

    /** 建会话结果。 */
    public record SessionOpening(String sessionId, String clientId) {
    }

    /** 注入客户端凭据登记册（Spring 装配使用）。 */
    public void setClientDirectory(ClientDirectory directory) {
        if (directory != null) {
            this.clientDirectory = directory;
        }
    }

    /** 断开会话：保留订阅状态与确认位点，未确认消息置为待重投。 */
    public void disconnect(String sessionId) {
        disconnect(sessionId, null);
    }

    /** 断开会话并核验调用方令牌（网络入口使用）。 */
    public void disconnect(String sessionId, String token) {
        Session session = requireToken(sessionId, token);
        session.gate.lock();
        try {
            session.connected = false;
            session.subscriptions.values().forEach(Subscription::requeueInFlight);
            session.available.signalAll(); // 唤醒可能在有上限等待的拉取
        } finally {
            session.gate.unlock();
        }
    }

    /** 重连已存在的会话；未知会话 SESSION_NOT_FOUND，已关闭 SESSION_CLOSED。 */
    public void reconnect(String sessionId) {
        reconnect(sessionId, null);
    }

    /** 重连并核验调用方令牌（网络入口使用）。 */
    public void reconnect(String sessionId, String token) {
        Session session = requireToken(sessionId, token);
        session.gate.lock();
        try {
            session.connected = true;
            // 被摘除的订阅重新挂上，补报摘除期间的缺口，随后从已确认位点续收。
            for (Subscription subscription : session.subscriptions.values()) {
                for (SeqGap gap : subscription.rearm()) {
                    recordEvent(session, SessionEvent.backlogGap(session.id, subscription.topic,
                            List.of(gap), "订阅重挂，摘除期间缺失序号"));
                }
            }
            session.available.signalAll();
        } finally {
            session.gate.unlock();
        }
    }

    /** 永久关闭会话：拒绝后续一切操作，重连也被拒绝；事件仍可被读取。 */
    public void closeSession(String sessionId) {
        closeSession(sessionId, null);
    }

    /** 永久关闭会话并核验调用方令牌（网络入口使用）。 */
    public void closeSession(String sessionId, String token) {
        Session session = requireToken(sessionId, token);
        session.gate.lock();
        try {
            if (!session.closed) {
                session.closed = true;
                session.connected = false;
                session.subscriptions.values().forEach(Subscription::requeueInFlight);
                recordEvent(session, SessionEvent.sessionClosed(session.id,
                        "会话被服务端关闭"));
                session.available.signalAll();
            }
        } finally {
            session.gate.unlock();
        }
    }

    /** 会话是否在线（未断线、未关闭）。 */
    public boolean isConnected(String sessionId) {
        Session session = session(sessionId);
        return session.connected && !session.closed;
    }

    /** 订阅主题（默认 DROP_OLDEST）：主题必须已注册且身份被授权。 */
    public void subscribe(String sessionId, String topic) {
        subscribe(sessionId, topic, BackpressureStrategy.DROP_OLDEST, null);
    }

    /** 订阅主题并选择背压策略（进程内用法）。 */
    public void subscribe(String sessionId, String topic, BackpressureStrategy strategy) {
        subscribe(sessionId, topic, strategy, null);
    }

    /** 订阅主题：网络入口，核验令牌。 */
    public void subscribe(String sessionId, String topic, BackpressureStrategy strategy, String token) {
        Session session = requireToken(sessionId, token);
        requireKnownTopic(topic);
        if (!session.identity.canSubscribe(topic)) {
            throw new BusException(ErrorCode.UNAUTHORIZED_SUBSCRIBE,
                    "会话 " + sessionId + " 无权订阅主题 " + topic);
        }
        BackpressureStrategy chosen = strategy == null ? BackpressureStrategy.DROP_OLDEST : strategy;
        session.gate.lock();
        try {
            requireActive(session);
            Subscription existing = session.subscriptions.get(topic);
            if (existing != null && existing.isEvicted()) {
                throw new BusException(ErrorCode.SUBSCRIPTION_EVICTED,
                        "会话 " + sessionId + " 对主题 " + topic + " 的订阅已被摘除，请重连后续挂");
            }
            session.subscriptions.computeIfAbsent(topic,
                    t -> new Subscription(session.id, t, chosen));
        } finally {
            session.gate.unlock();
        }
    }

    /**
     * 发布消息，返回主题内序号（进程内用法）。
     * 空报文、超限报文、未知主题、未授权发布、断线均以可区分错误拒绝。
     */
    public long publish(String sessionId, String topic, byte[] payload) {
        return publish(sessionId, topic, payload, null);
    }

    /** 发布消息：网络入口，核验令牌。 */
    public long publish(String sessionId, String topic, byte[] payload, String token) {
        Session publisher = requireToken(sessionId, token);
        requireKnownTopic(topic);
        if (!publisher.identity.canPublish(topic)) {
            throw new BusException(ErrorCode.UNAUTHORIZED_PUBLISH,
                    "会话 " + sessionId + " 无权向主题 " + topic + " 发布");
        }
        validatePayload(topic, payload);
        if (publisher.closed) {
            throw new BusException(ErrorCode.SESSION_CLOSED, "会话 " + sessionId + " 已关闭");
        }
        if (!publisher.connected) {
            throw new BusException(ErrorCode.SESSION_DISCONNECTED,
                    "会话 " + sessionId + " 已断线，发布被拒绝，请重连后再试");
        }
        long seq = topicSequences.get(topic).incrementAndGet();
        Message message = new Message(topic, seq, payload, clock.millis());
        fanOut(message);
        return seq;
    }

    /** 扇出到该主题的所有订阅者；每个订阅者在自己的 gate 锁内处理，互不阻塞。 */
    private void fanOut(Message message) {
        for (Session subscriber : sessions.values()) {
            Subscription subscription = subscriber.subscriptions.get(message.topic());
            if (subscription == null) {
                continue;
            }
            subscriber.gate.lock();
            try {
                if (subscriber.closed) {
                    continue;
                }
                enqueueForSession(subscriber, subscription, message);
                subscriber.available.signalAll();
            } finally {
                subscriber.gate.unlock();
            }
        }
    }

    /**
     * 在会话 gate 锁内把消息送入目标订阅：先过会话总量阀，再走订阅自身策略。
     * 断线订阅者照常缓冲，重连后可继续投递；摘除中的订阅只跟踪缺口不占缓冲。
     */
    private void enqueueForSession(Session session, Subscription target, Message message) {
        if (message.seq() <= target.lastAckedSeq()) {
            return;
        }
        if (target.isEvicted()) {
            // 摘除中的订阅不占名额：仅由订阅自身跟踪序号（用于重挂补报缺口），不缓冲。
            target.enqueue(message, config);
            return;
        }
        // 会话总量阀：确保缓冲（含本消息）不超过会话总上限。
        while (bufferedTotal(session) >= config.maxBacklogPerSession()) {
            Subscription victim = oldestPendingSubscription(session);
            if (victim == null) {
                // 没有待投递积压（名额只被未确认占用，且摘除中的订阅不计数）：
                // 直接尝试入队，由订阅自身策略决定，避免误摘正常订阅。
                break;
            }
            Subscription.Occurrence valve = victim.dropOldestForSessionValve(
                    config.maxBacklogPerSession());
            if (valve != null) {
                appendOccurrences(session, List.of(valve));
            }
            if (target.isEvicted()) {
                return; // 目标订阅被阀摘除并作废了该消息，不再入队
            }
        }
        Subscription.EnqueueOutcome outcome = target.enqueue(message, config);
        appendOccurrences(session, outcome.occurrences);
    }

    /** 会话全部订阅当前合计占用的缓冲。 */
    private int bufferedTotal(Session session) {
        int total = 0;
        for (Subscription subscription : session.subscriptions.values()) {
            total += subscription.bufferedCount();
        }
        return total;
    }

    /** 会话内待投递积压序号最旧的订阅（供总量阀选择牺牲者）。 */
    private Subscription oldestPendingSubscription(Session session) {
        Subscription victim = null;
        long oldest = Long.MAX_VALUE;
        for (Subscription subscription : session.subscriptions.values()) {
            long seq = subscription.oldestPendingSeq();
            if (seq < oldest) {
                oldest = seq;
                victim = subscription;
            }
        }
        return victim;
    }

    /** 非阻塞拉取（兼容既有签名）：先超时重投，再按序投递新消息。 */
    public List<Delivery> poll(String sessionId, String topic, int max) {
        return poll(sessionId, topic, max, 0L, null);
    }

    /**
     * 拉取投递：先重投超时未确认消息（redelivered=true），再按序投递新消息。
     *
     * @param waitMillis 没有新消息时，最多在本会话自己的条件变量上等待的毫秒数，
     *                   0 表示不等待；等待有上限，只阻塞调用方自己，不影响其他会话
     * @param token      调用方令牌（网络入口），进程内用法传 null
     */
    public List<Delivery> poll(String sessionId, String topic, int max, long waitMillis, String token) {
        Session session = requireToken(sessionId, token);
        requireKnownTopic(topic);
        Subscription subscription = requireSubscription(session, topic);
        long deadlineNanos = waitMillis > 0
                ? System.nanoTime() + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(waitMillis)
                : 0L;
        session.gate.lock();
        try {
            while (true) {
                if (session.closed) {
                    throw new BusException(ErrorCode.SESSION_CLOSED,
                            "会话 " + sessionId + " 已关闭");
                }
                if (!session.connected) {
                    throw new BusException(ErrorCode.SESSION_DISCONNECTED,
                            "会话 " + sessionId + " 已断线，拉取被拒绝，请重连后再试");
                }
                if (subscription.isEvicted()) {
                    throw new BusException(ErrorCode.SUBSCRIPTION_EVICTED,
                            "会话 " + sessionId + " 对主题 " + topic + " 的订阅已被摘除");
                }
                List<Delivery> deliveries = collectDeliveries(subscription, max);
                if (!deliveries.isEmpty() || waitMillis <= 0L) {
                    return deliveries;
                }
                long remaining = deadlineNanos - System.nanoTime();
                if (remaining <= 0L) {
                    return deliveries;
                }
                session.available.awaitNanos(remaining);
                // 被唤醒后重新检查连接/摘除状态与消息
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BusException(ErrorCode.MALFORMED_MESSAGE, "拉取被中断");
        } finally {
            session.gate.unlock();
        }
    }

    /** 在 gate 锁内收集一次投递：超时重投优先，随后按序新消息。 */
    private List<Delivery> collectDeliveries(Subscription subscription, int max) {
        long now = clock.millis();
        List<Delivery> out = new ArrayList<>(Math.max(max, 0));
        for (Message message : subscription.dueRedeliveries(now)) {
            if (out.size() >= max) {
                break;
            }
            subscription.markDelivered(message, now, config.ackTimeoutMillis());
            out.add(new Delivery(message, true, subscription.attemptOf(message.seq())));
        }
        for (Message message : subscription.takePending(max - out.size())) {
            subscription.markDelivered(message, now, config.ackTimeoutMillis());
            out.add(new Delivery(message, false, subscription.attemptOf(message.seq())));
        }
        return out;
    }

    /** 累积确认（进程内用法）。 */
    public void ack(String sessionId, String topic, long seq) {
        ack(sessionId, topic, seq, null);
    }

    /** 累积确认：网络入口，核验令牌。 */
    public void ack(String sessionId, String topic, long seq, String token) {
        Session session = requireToken(sessionId, token);
        requireKnownTopic(topic);
        Subscription subscription = requireSubscription(session, topic);
        session.gate.lock();
        try {
            if (session.closed) {
                throw new BusException(ErrorCode.SESSION_CLOSED, "会话 " + sessionId + " 已关闭");
            }
            subscription.ackUpTo(seq);
        } finally {
            session.gate.unlock();
        }
    }

    /** 读取并清空该会话的旧模型丢弃/摘除事件（兼容既有用法与测试）。 */
    public List<DropEvent> drainDropEvents(String sessionId) {
        Session session = session(sessionId);
        session.gate.lock();
        try {
            List<DropEvent> snapshot = new ArrayList<>(session.dropEvents);
            session.dropEvents.clear();
            return snapshot;
        } finally {
            session.gate.unlock();
        }
    }

    /** 读取并清空该会话的新模型事件（缺口 / 摘除 / 关闭）。 */
    public List<SessionEvent> drainEvents(String sessionId) {
        return drainEvents(sessionId, null);
    }

    /** 读取并清空事件：网络入口，核验令牌。关闭的会话也允许读取其最后的关闭事件。 */
    public List<SessionEvent> drainEvents(String sessionId, String token) {
        Session session = session(sessionId);
        checkTokenOnly(session, token);
        session.gate.lock();
        try {
            List<SessionEvent> snapshot = new ArrayList<>(session.events);
            session.events.clear();
            return snapshot;
        } finally {
            session.gate.unlock();
        }
    }

    /** 当前确认位点（用于测试与可观测性）。 */
    public long ackedPosition(String sessionId, String topic) {
        Session session = session(sessionId);
        Subscription subscription = session.subscriptions.get(topic);
        return subscription == null ? 0L : subscription.lastAckedSeq();
    }

    // ---- 内部辅助 -------------------------------------------------------

    private void appendOccurrences(Session session, List<Subscription.Occurrence> occurrences) {
        for (Subscription.Occurrence occurrence : occurrences) {
            session.dropEvents.addAll(toLegacy(occurrence));
            // 摘除时必须让“丢了哪些序号”与“订阅被摘除”两类事件彼此可见：
            // 先补一条缺口事件（若有作废序号），再发摘除事件（摘除事件也携带缺口便于关联）。
            if (occurrence.kind() != Subscription.Occurrence.Kind.OVERFLOW
                    && !occurrence.gaps().isEmpty()) {
                session.events.add(SessionEvent.backlogGap(session.id, occurrence.topic(),
                        occurrence.gaps(), occurrence.detail()));
            }
            session.events.add(toEvent(session.id, occurrence));
        }
    }

    private void recordEvent(Session session, SessionEvent event) {
        session.events.add(event);
    }

    private static SessionEvent toEvent(String sessionId, Subscription.Occurrence o) {
        return switch (o.kind()) {
            case OVERFLOW -> SessionEvent.backlogGap(sessionId, o.topic(), o.gaps(), o.detail());
            case EVICT_THRESHOLD, EVICT_STRATEGY ->
                    SessionEvent.subscriptionEvicted(sessionId, o.topic(), o.gaps(), o.detail());
        };
    }

    /**
     * 旧模型只表达“丢了某条 / 连续丢弃导致摘除”：
     * OVERFLOW -> BACKLOG_OVERFLOW(seq)；阈值摘除 -> SLOW_CONSUMER_EVICTED(seq)；
     * EVICT 策略摘除在旧模型中也归一为 SLOW_CONSUMER_EVICTED。
     */
    private static List<DropEvent> toLegacy(Subscription.Occurrence o) {
        return switch (o.kind()) {
            case OVERFLOW -> List.of(new DropEvent("", o.topic(), o.seq(),
                    DropReason.BACKLOG_OVERFLOW, o.detail()));
            case EVICT_THRESHOLD -> List.of(new DropEvent("", o.topic(), o.seq(),
                    DropReason.SLOW_CONSUMER_EVICTED, o.detail()));
            case EVICT_STRATEGY -> List.of(); // 旧用法没有“直接摘除”语义，只走新事件 API
        };
    }

    private Session session(String sessionId) {
        Session session = sessions.get(sessionId);
        if (session == null) {
            throw new BusException(ErrorCode.SESSION_NOT_FOUND, "未知会话 " + sessionId);
        }
        return session;
    }

    /** 核验会话存在、未关闭，并在提供令牌时核验令牌归属。 */
    private Session requireToken(String sessionId, String token) {
        Session session = session(sessionId);
        if (token != null && (session.token == null || !session.token.equals(token))) {
            throw new BusException(ErrorCode.INVALID_CREDENTIALS,
                    "令牌与会话 " + sessionId + " 归属不匹配");
        }
        if (session.closed) {
            throw new BusException(ErrorCode.SESSION_CLOSED, "会话 " + sessionId + " 已关闭");
        }
        return session;
    }

    /** 仅核验令牌归属，不要求会话在线（供读取事件使用）。 */
    private void checkTokenOnly(Session session, String token) {
        if (token != null && (session.token == null || !session.token.equals(token))) {
            throw new BusException(ErrorCode.INVALID_CREDENTIALS,
                    "令牌与会话 " + session.id + " 归属不匹配");
        }
    }

    private void requireActive(Session session) {
        if (session.closed) {
            throw new BusException(ErrorCode.SESSION_CLOSED, "会话 " + session.id + " 已关闭");
        }
        if (!session.connected) {
            throw new BusException(ErrorCode.SESSION_DISCONNECTED,
                    "会话 " + session.id + " 已断线，请重连后再试");
        }
    }

    private Subscription requireSubscription(Session session, String topic) {
        Subscription subscription = session.subscriptions.get(topic);
        if (subscription == null) {
            throw new BusException(ErrorCode.SUBSCRIPTION_NOT_FOUND,
                    "会话 " + session.id + " 未订阅主题 " + topic);
        }
        return subscription;
    }

    private void requireKnownTopic(String topic) {
        if (!topics.contains(topic)) {
            throw new BusException(ErrorCode.UNKNOWN_TOPIC, "未知主题 " + topic);
        }
    }

    private void validatePayload(String topic, byte[] payload) {
        if (payload == null || payload.length == 0) {
            throw new BusException(ErrorCode.MALFORMED_MESSAGE,
                    "主题 " + topic + " 收到空报文");
        }
        if (payload.length > config.maxPayloadBytes()) {
            throw new BusException(ErrorCode.MESSAGE_TOO_LARGE,
                    "报文 " + payload.length + " 字节超过上限 " + config.maxPayloadBytes());
        }
    }
}
