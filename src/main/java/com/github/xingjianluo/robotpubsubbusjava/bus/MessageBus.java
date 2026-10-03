package com.github.xingjianluo.robotpubsubbusjava.bus;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 本地发布/订阅消息总线。
 *
 * <p>核心语义：
 * <ul>
 *   <li>会话内按主题序号有序投递，至少一次（at-least-once）；</li>
 *   <li>断线保留订阅状态，重连后从已确认位点继续，未确认消息超时重投；
 *       断线期间该会话的发布/拉取被明确挡住（{@link ErrorCode#SESSION_DISCONNECTED}）；</li>
 *   <li>单订阅积压有界，会话全部订阅积压之和另有总上限
 *       （{@link BusConfig#maxBacklogPerSession}），不随订阅数无界增长；</li>
 *   <li>超限按订阅选择的 {@link BackpressurePolicy} 处理：丢弃最旧或摘除订阅；
 *       订阅被摘除后会话保留，重连后从已确认位点续传；</li>
 *   <li>投递结果以 {@link BusEvent} 对客户端可见：正常投递、背压缺口、
 *       订阅摘除、会话关闭彼此可区分；</li>
 *   <li>发布/订阅按服务端确认的会话身份做主题级授权，所有失败以
 *       {@link ErrorCode} 区分并显式抛出，不静默断开、不静默丢消息。</li>
 * </ul>
 */
public class MessageBus {

    private static final class Session {
        final String id;
        final SessionIdentity identity;
        /** key: topic */
        final Map<String, Subscription> subscriptions = new ConcurrentHashMap<>();
        final List<DropEvent> dropEvents = new CopyOnWriteArrayList<>();
        /** 客户端可见事件队列；GAP 事件入队时与队尾相邻缺口合并 */
        private final List<BusEvent> events = new ArrayList<>();
        volatile boolean connected = true;
        volatile boolean closed = false;

        Session(String id, SessionIdentity identity) {
            this.id = id;
            this.identity = identity;
        }

        void addEvent(BusEvent event) {
            synchronized (events) {
                int lastIndex = events.size() - 1;
                if (event.kind() == EventKind.GAP && lastIndex >= 0) {
                    BusEvent tail = events.get(lastIndex);
                    if (tail.kind() == EventKind.GAP
                            && tail.topic().equals(event.topic())
                            && tail.toSeq() + 1 == event.fromSeq()) {
                        events.set(lastIndex, BusEvent.gap(id, event.topic(),
                                tail.fromSeq(), event.toSeq(), tail.detail()));
                        return;
                    }
                }
                events.add(event);
            }
        }

        List<BusEvent> drainEvents() {
            synchronized (events) {
                List<BusEvent> out = new ArrayList<>(events);
                events.clear();
                return out;
            }
        }

        int totalPending() {
            int total = 0;
            for (Subscription subscription : subscriptions.values()) {
                total += subscription.pendingCount();
            }
            return total;
        }
    }

    private final BusConfig config;
    private final Clock clock;
    private final SessionAuthenticator authenticator;
    private final Set<String> topics = ConcurrentHashMap.newKeySet();
    private final Map<String, Session> sessions = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> topicSequences = new ConcurrentHashMap<>();
    /** 每主题一把锁：序号分配与投递入队在同一临界区，并发发布不会串位 */
    private final Map<String, Object> topicLocks = new ConcurrentHashMap<>();

    public MessageBus(BusConfig config, Clock clock) {
        this(config, clock, null);
    }

    public MessageBus(BusConfig config, Clock clock, SessionAuthenticator authenticator) {
        this.config = config;
        this.clock = clock;
        this.authenticator = authenticator;
    }

    /** 注册主题；未注册主题的发布/订阅将以 UNKNOWN_TOPIC 拒绝。 */
    public void registerTopic(String topic) {
        topics.add(topic);
        topicSequences.putIfAbsent(topic, new AtomicLong(0));
        topicLocks.putIfAbsent(topic, new Object());
    }

    /** 当前已注册主题清单（升序）。 */
    public List<String> listTopics() {
        return new ArrayList<>(new TreeSet<>(topics));
    }

    /** 以服务端确认的身份开启会话（进程内直接调用），返回会话标识。 */
    public String openSession(SessionIdentity identity) {
        String sessionId = identity.clientId() + "-" + UUID.randomUUID();
        sessions.put(sessionId, new Session(sessionId, identity));
        return sessionId;
    }

    /**
     * 以客户端凭据开启会话：凭据由服务端配置的核验器认下并核验为身份，
     * 客户端不能自称身份；未携带凭据或凭据非法直接拒绝。
     */
    public String openSession(String credential) {
        if (authenticator == null) {
            throw new BusException(ErrorCode.AUTHENTICATION_FAILED, "服务端未配置身份核验器");
        }
        return openSession(authenticator.authenticate(credential));
    }

    /** 断开会话：保留订阅状态与确认位点，未确认消息置为待重投；此后发布/拉取被挡住，
     *  正在等待中的拉取被唤醒并以 SESSION_DISCONNECTED 拒绝。 */
    public void disconnect(String sessionId) {
        Session session = activeSession(sessionId);
        session.connected = false;
        session.subscriptions.values().forEach(subscription -> {
            subscription.requeueInFlight();
            subscription.wakeup();
        });
    }

    /** 重连已存在的会话；未知会话以 SESSION_NOT_FOUND 拒绝；被摘除的订阅随重连复活续传。 */
    public void reconnect(String sessionId) {
        Session session = activeSession(sessionId);
        session.connected = true;
        session.subscriptions.values().forEach(Subscription::revive);
    }

    /** 关闭会话：终态，不可再重连；事件队列保留 SESSION_CLOSED 供客户端最后读取，
     *  正在等待中的拉取被唤醒并以 SESSION_CLOSED 拒绝。 */
    public void closeSession(String sessionId) {
        Session session = activeSession(sessionId);
        session.connected = false;
        session.closed = true;
        session.subscriptions.values().forEach(Subscription::wakeup);
        session.addEvent(BusEvent.sessionClosed(sessionId, "会话被服务端关闭"));
    }

    /** 订阅主题（默认 DROP_OLDEST 背压策略，兼容旧用法）。 */
    public void subscribe(String sessionId, String topic) {
        subscribe(sessionId, topic, BackpressurePolicy.DROP_OLDEST);
    }

    /** 订阅主题并选择背压策略：主题必须已注册且身份被授权，否则显式拒绝。 */
    public void subscribe(String sessionId, String topic, BackpressurePolicy policy) {
        Session session = connectedSession(sessionId);
        requireKnownTopic(topic);
        if (!session.identity.canSubscribe(topic)) {
            throw new BusException(ErrorCode.UNAUTHORIZED_SUBSCRIBE,
                    "会话 " + sessionId + " 无权订阅主题 " + topic);
        }
        session.subscriptions.computeIfAbsent(topic,
                t -> new Subscription(sessionId, t, policy));
    }

    /**
     * 发布消息，返回主题内序号。
     * 断线/关闭会话、空报文、超限报文、未知主题、未授权发布均以可区分错误拒绝。
     */
    public long publish(String sessionId, String topic, byte[] payload) {
        Session session = connectedSession(sessionId);
        requireKnownTopic(topic);
        if (!session.identity.canPublish(topic)) {
            throw new BusException(ErrorCode.UNAUTHORIZED_PUBLISH,
                    "会话 " + sessionId + " 无权向主题 " + topic + " 发布");
        }
        if (payload == null || payload.length == 0) {
            throw new BusException(ErrorCode.MALFORMED_MESSAGE,
                    "主题 " + topic + " 收到空报文");
        }
        if (payload.length > config.maxPayloadBytes()) {
            throw new BusException(ErrorCode.MESSAGE_TOO_LARGE,
                    "报文 " + payload.length + " 字节超过上限 " + config.maxPayloadBytes());
        }
        // 序号分配与投递入队必须在同一临界区：否则并发发布可能先拿序号后入队，
        // 导致订阅方看到的序号顺序与分配顺序不一致（串位）
        Object topicLock = topicLocks.get(topic);
        synchronized (topicLock) {
            long seq = topicSequences.get(topic).incrementAndGet();
            Message message = new Message(topic, seq, payload, clock.millis());
            for (Session subscriber : sessions.values()) {
                Subscription subscription = subscriber.subscriptions.get(topic);
                if (subscription == null) {
                    continue;
                }
                // 会话级总量检查与入队在会话锁内完成，并发发布不会突破总上限
                Subscription.EnqueueResult result;
                synchronized (subscriber) {
                    int room = config.maxBacklogPerSession() - subscriber.totalPending();
                    result = subscription.enqueue(message, config, Math.max(room, 0));
                }
                if (result.dropEvent() != null) {
                    subscriber.dropEvents.add(result.dropEvent());
                }
                result.events().forEach(subscriber::addEvent);
            }
            return seq;
        }
    }

    /** 拉取投递（不等待），等价于 {@code poll(sessionId, topic, max, 0)}。 */
    public List<Delivery> poll(String sessionId, String topic, int max) {
        return poll(sessionId, topic, max, 0L);
    }

    /**
     * 拉取投递：先重投超时未确认的消息（redelivered=true），
     * 再按序投递新消息（redelivered=false）。
     *
     * <p>{@code waitMillis} 允许调用方在没有可投递内容时有界等待，
     * 实际等待不超过 {@link BusConfig#maxPollWaitMillis}，且只阻塞在
     * 该订阅自身的监视器上，不会拖住其他会话或订阅。
     *
     * <p>等待期间会话被断开/关闭的，等待被立即打断并以
     * {@link ErrorCode#SESSION_DISCONNECTED}/{@link ErrorCode#SESSION_CLOSED}
     * 拒绝：断线生效后任何拉取（含重投消息）都不会再返回，
     * 只有重连成功之后才能继续收。
     */
    public List<Delivery> poll(String sessionId, String topic, int max, long waitMillis) {
        Session session = connectedSession(sessionId);
        Subscription subscription = subscription(session, topic);
        requireNotEvicted(sessionId, topic, subscription);
        long budget = Math.min(Math.max(waitMillis, 0L), config.maxPollWaitMillis());
        long deadlineNanos = System.nanoTime() + budget * 1_000_000L;
        while (true) {
            // 每一轮（含等待被唤醒后）都先复查会话状态：断线生效后不再返回任何消息
            requireConnected(session);
            List<Delivery> out = collect(session, subscription, max);
            if (!out.isEmpty() || budget <= 0L) {
                return out;
            }
            long remainingMillis = (deadlineNanos - System.nanoTime()) / 1_000_000L;
            if (remainingMillis <= 0L) {
                return out;
            }
            try {
                subscription.await(remainingMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return out;
            }
            requireNotEvicted(sessionId, topic, subscription);
        }
    }

    private List<Delivery> collect(Session session, Subscription subscription, int max) {
        long now = clock.millis();
        List<Delivery> out = new ArrayList<>(max);
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
        for (Delivery delivery : out) {
            session.addEvent(BusEvent.delivered(session.id,
                    subscription.topic, delivery.message().seq()));
        }
        return out;
    }

    /** 累积确认：确认位点推进到 seq（含），已确认消息不再投递。 */
    public void ack(String sessionId, String topic, long seq) {
        Session session = connectedSession(sessionId);
        subscription(session, topic).ackUpTo(seq);
    }

    /** 读取并清空该会话的丢弃/断开事件（兼容旧版 DropEvent 视图）。 */
    public List<DropEvent> drainDropEvents(String sessionId) {
        Session session = session(sessionId);
        List<DropEvent> events = new ArrayList<>(session.dropEvents);
        session.dropEvents.clear();
        return events;
    }

    /** 读取并清空该会话的客户端可见事件（投递/缺口/摘除/关闭，彼此可区分）。 */
    public List<BusEvent> drainEvents(String sessionId) {
        return session(sessionId).drainEvents();
    }

    /** 当前确认位点（用于测试与可观测性）。 */
    public long ackedPosition(String sessionId, String topic) {
        return subscription(session(sessionId), topic).lastAckedSeq();
    }

    /** 服务端认下的会话身份标识（用于可观测性与建会话响应）。 */
    public String clientId(String sessionId) {
        return session(sessionId).identity.clientId();
    }

    /** 该会话某订阅当前积压条数（用于测试与可观测性）。 */
    public int pendingCount(String sessionId, String topic) {
        return subscription(session(sessionId), topic).pendingCount();
    }

    /** 该会话全部订阅积压之和（受 maxBacklogPerSession 约束）。 */
    public int totalPending(String sessionId) {
        return session(sessionId).totalPending();
    }

    private void requireNotEvicted(String sessionId, String topic, Subscription subscription) {
        if (subscription.isEvicted()) {
            throw new BusException(ErrorCode.SUBSCRIPTION_EVICTED,
                    "会话 " + sessionId + " 对主题 " + topic + " 的订阅已因慢消费者被断开");
        }
    }

    private Session session(String sessionId) {
        Session session = sessions.get(sessionId);
        if (session == null) {
            throw new BusException(ErrorCode.SESSION_NOT_FOUND, "未知会话 " + sessionId);
        }
        return session;
    }

    /** 会话必须存在且未被关闭。 */
    private Session activeSession(String sessionId) {
        Session session = session(sessionId);
        if (session.closed) {
            throw new BusException(ErrorCode.SESSION_CLOSED, "会话 " + sessionId + " 已被关闭");
        }
        return session;
    }

    /** 会话必须存在、未关闭且处于连接状态；断线会话的数据操作被明确挡住。 */
    private Session connectedSession(String sessionId) {
        Session session = activeSession(sessionId);
        requireConnected(session);
        return session;
    }

    /** 等待中的拉取每轮复查用：断线/关闭立即以可区分错误拒绝。 */
    private void requireConnected(Session session) {
        if (session.closed) {
            throw new BusException(ErrorCode.SESSION_CLOSED, "会话 " + session.id + " 已被关闭");
        }
        if (!session.connected) {
            throw new BusException(ErrorCode.SESSION_DISCONNECTED,
                    "会话 " + session.id + " 处于断线状态，需重连后继续");
        }
    }

    private Subscription subscription(Session session, String topic) {
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
}
