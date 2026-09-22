package com.github.xingjianluo.robotpubsubbusjava.bus;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
 *   <li>断线保留订阅状态，重连后从已确认位点继续，未确认消息超时重投；</li>
 *   <li>单订阅积压有界，超限丢弃最旧消息，连续丢弃超限则断开该订阅，
 *       不影响其他订阅者；</li>
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
        volatile boolean connected = true;

        Session(String id, SessionIdentity identity) {
            this.id = id;
            this.identity = identity;
        }
    }

    private final BusConfig config;
    private final Clock clock;
    private final Set<String> topics = ConcurrentHashMap.newKeySet();
    private final Map<String, Session> sessions = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> topicSequences = new ConcurrentHashMap<>();

    public MessageBus(BusConfig config, Clock clock) {
        this.config = config;
        this.clock = clock;
    }

    /** 注册主题；未注册主题的发布/订阅将以 UNKNOWN_TOPIC 拒绝。 */
    public void registerTopic(String topic) {
        topics.add(topic);
        topicSequences.putIfAbsent(topic, new AtomicLong(0));
    }

    /** 以服务端确认的身份开启会话，返回会话标识。 */
    public String openSession(SessionIdentity identity) {
        String sessionId = identity.clientId() + "-" + UUID.randomUUID();
        sessions.put(sessionId, new Session(sessionId, identity));
        return sessionId;
    }

    /** 断开会话：保留订阅状态与确认位点，未确认消息置为待重投。 */
    public void disconnect(String sessionId) {
        Session session = session(sessionId);
        session.connected = false;
        session.subscriptions.values().forEach(Subscription::requeueInFlight);
    }

    /** 重连已存在的会话；未知会话以 SESSION_NOT_FOUND 拒绝。 */
    public void reconnect(String sessionId) {
        Session session = session(sessionId);
        session.connected = true;
        session.subscriptions.values().forEach(Subscription::resetDrops);
    }

    /** 订阅主题：主题必须已注册且身份被授权，否则显式拒绝。 */
    public void subscribe(String sessionId, String topic) {
        Session session = session(sessionId);
        requireKnownTopic(topic);
        if (!session.identity.canSubscribe(topic)) {
            throw new BusException(ErrorCode.UNAUTHORIZED_SUBSCRIBE,
                    "会话 " + sessionId + " 无权订阅主题 " + topic);
        }
        session.subscriptions.computeIfAbsent(topic, t -> new Subscription(sessionId, t));
    }

    /**
     * 发布消息，返回主题内序号。
     * 空报文、超限报文、未知主题、未授权发布均以可区分错误拒绝。
     */
    public long publish(String sessionId, String topic, byte[] payload) {
        Session session = session(sessionId);
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
        long seq = topicSequences.get(topic).incrementAndGet();
        Message message = new Message(topic, seq, payload, clock.millis());
        for (Session subscriber : sessions.values()) {
            Subscription subscription = subscriber.subscriptions.get(topic);
            if (subscription == null) {
                continue;
            }
            DropEvent event = subscription.enqueue(message, config);
            if (event != null) {
                subscriber.dropEvents.add(event);
            }
        }
        return seq;
    }

    /**
     * 拉取投递：先重投超时未确认的消息（redelivered=true），
     * 再按序投递新消息（redelivered=false）。
     */
    public List<Delivery> poll(String sessionId, String topic, int max) {
        Subscription subscription = subscription(sessionId, topic);
        if (subscription.isEvicted()) {
            throw new BusException(ErrorCode.SUBSCRIPTION_EVICTED,
                    "会话 " + sessionId + " 对主题 " + topic + " 的订阅已因慢消费者被断开");
        }
        long now = clock.millis();
        List<Delivery> out = new ArrayList<>(max);
        for (Message message : subscription.dueRedeliveries(now)) {
            if (out.size() >= max) {
                break;
            }
            subscription.markDelivered(message, now, config.ackTimeoutMillis(), true);
            out.add(new Delivery(message, true, subscription.attemptOf(message.seq())));
        }
        for (Message message : subscription.takePending(max - out.size())) {
            subscription.markDelivered(message, now, config.ackTimeoutMillis(), false);
            out.add(new Delivery(message, false, subscription.attemptOf(message.seq())));
        }
        return out;
    }

    /** 累积确认：确认位点推进到 seq（含），已确认消息不再投递。 */
    public void ack(String sessionId, String topic, long seq) {
        subscription(sessionId, topic).ackUpTo(seq);
    }

    /** 读取并清空该会话的丢弃/断开事件。 */
    public List<DropEvent> drainDropEvents(String sessionId) {
        Session session = session(sessionId);
        List<DropEvent> events = new ArrayList<>(session.dropEvents);
        session.dropEvents.clear();
        return events;
    }

    /** 当前确认位点（用于测试与可观测性）。 */
    public long ackedPosition(String sessionId, String topic) {
        return subscription(sessionId, topic).lastAckedSeq();
    }

    private Session session(String sessionId) {
        Session session = sessions.get(sessionId);
        if (session == null) {
            throw new BusException(ErrorCode.SESSION_NOT_FOUND, "未知会话 " + sessionId);
        }
        return session;
    }

    private Subscription subscription(String sessionId, String topic) {
        Subscription subscription = session(sessionId).subscriptions.get(topic);
        if (subscription == null) {
            throw new BusException(ErrorCode.SUBSCRIPTION_NOT_FOUND,
                    "会话 " + sessionId + " 未订阅主题 " + topic);
        }
        return subscription;
    }

    private void requireKnownTopic(String topic) {
        if (!topics.contains(topic)) {
            throw new BusException(ErrorCode.UNKNOWN_TOPIC, "未知主题 " + topic);
        }
    }
}
