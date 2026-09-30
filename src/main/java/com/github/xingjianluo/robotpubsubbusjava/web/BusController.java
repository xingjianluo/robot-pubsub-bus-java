package com.github.xingjianluo.robotpubsubbusjava.web;

import com.github.xingjianluo.robotpubsubbusjava.bus.BusException;
import com.github.xingjianluo.robotpubsubbusjava.bus.Delivery;
import com.github.xingjianluo.robotpubsubbusjava.bus.ErrorCode;
import com.github.xingjianluo.robotpubsubbusjava.bus.MessageBus;
import com.github.xingjianluo.robotpubsubbusjava.bus.SessionEvent;
import com.github.xingjianluo.robotpubsubbusjava.web.ApiDtos.AckRequest;
import com.github.xingjianluo.robotpubsubbusjava.web.ApiDtos.AckResponse;
import com.github.xingjianluo.robotpubsubbusjava.web.ApiDtos.EventsResponse;
import com.github.xingjianluo.robotpubsubbusjava.web.ApiDtos.OpenSessionResponse;
import com.github.xingjianluo.robotpubsubbusjava.web.ApiDtos.PollResponse;
import com.github.xingjianluo.robotpubsubbusjava.web.ApiDtos.PublishRequest;
import com.github.xingjianluo.robotpubsubbusjava.web.ApiDtos.PublishResponse;
import com.github.xingjianluo.robotpubsubbusjava.web.ApiDtos.SubscribeRequest;
import com.github.xingjianluo.robotpubsubbusjava.web.ApiDtos.TopicsResponse;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Base64;
import java.util.List;

/**
 * 总线对外 HTTP 入口。
 *
 * <p>鉴权约定：
 * <ul>
 *   <li>{@code POST /api/sessions} 带 {@code X-Client-Token} 建会话，身份由服务端凭令牌核验；
 *       缺令牌 401 UNAUTHENTICATED，令牌不合法 403 INVALID_CREDENTIALS；</li>
 *   <li>其余所有接口路径携带 sessionId 且必须带回本人令牌，令牌与归属不匹配 403；</li>
 *   <li>越权发布/订阅与未鉴权是不同错误码（UNAUTHORIZED_* / UNAUTHENTICATED /
 *       INVALID_CREDENTIALS），未知主题为 UNKNOWN_TOPIC。</li>
 * </ul>
 */
@RestController
@RequestMapping("/api")
public class BusController {

    private static final String TOKEN_HEADER = "X-Client-Token";

    private final MessageBus bus;
    private final long pollWaitMillis;

    public BusController(MessageBus bus, BusProperties properties) {
        this.bus = bus;
        this.pollWaitMillis = properties.getPollWaitMillis();
    }

    /** 建会话：服务端凭令牌核验并认下身份，返回服务端生成的会话标识。 */
    @PostMapping("/sessions")
    public OpenSessionResponse openSession(
            @RequestHeader(value = TOKEN_HEADER, required = false) String token) {
        MessageBus.SessionOpening opening = bus.authenticate(token);
        return new OpenSessionResponse(opening.sessionId(), opening.clientId());
    }

    /** 主题清单。 */
    @GetMapping("/topics")
    public TopicsResponse listTopics() {
        return new TopicsResponse(bus.listTopics());
    }

    /** 订阅主题，可按订阅选择背压策略。 */
    @PostMapping("/sessions/{sessionId}/subscriptions")
    public void subscribe(@PathVariable String sessionId,
                          @RequestHeader(value = TOKEN_HEADER, required = false) String token,
                          @RequestBody SubscribeRequest request) {
        requireBody(request != null && request.topic() != null && !request.topic().isBlank(),
                "订阅请求缺少 topic");
        bus.subscribe(sessionId, request.topic(), request.strategy(), token);
    }

    /** 发布消息，负载为 Base64。 */
    @PostMapping("/sessions/{sessionId}/messages")
    public PublishResponse publish(@PathVariable String sessionId,
                                   @RequestHeader(value = TOKEN_HEADER, required = false) String token,
                                   @RequestBody PublishRequest request) {
        requireBody(request != null && request.topic() != null && !request.topic().isBlank(),
                "发布请求缺少 topic");
        byte[] payload = decodePayload(request.payloadBase64());
        long seq = bus.publish(sessionId, request.topic(), payload, token);
        return new PublishResponse(request.topic(), seq);
    }

    /** 拉取投递；wait=true 时在服务端有上限地等待新消息，只阻塞该会话自己。 */
    @GetMapping("/sessions/{sessionId}/subscriptions/{topic}/messages")
    public PollResponse poll(@PathVariable String sessionId,
                             @PathVariable String topic,
                             @RequestHeader(value = TOKEN_HEADER, required = false) String token,
                             @RequestParam(value = "max", defaultValue = "100") int max,
                             @RequestParam(value = "wait", defaultValue = "false") boolean wait) {
        long waitMillis = wait ? pollWaitMillis : 0L;
        List<Delivery> deliveries = bus.poll(sessionId, topic, max, waitMillis, token);
        return new PollResponse(deliveries.stream().map(ApiDtos.DeliveryDto::from).toList());
    }

    /** 累积确认。 */
    @PostMapping("/sessions/{sessionId}/acks")
    public AckResponse ack(@PathVariable String sessionId,
                           @RequestHeader(value = TOKEN_HEADER, required = false) String token,
                           @RequestBody AckRequest request) {
        requireBody(request != null && request.topic() != null && !request.topic().isBlank(),
                "确认请求缺少 topic");
        bus.ack(sessionId, request.topic(), request.seq(), token);
        return new AckResponse(request.topic(), request.seq());
    }

    /** 读取并清空事件（缺口 / 订阅摘除 / 会话关闭）。 */
    @GetMapping("/sessions/{sessionId}/events")
    public EventsResponse events(@PathVariable String sessionId,
                                 @RequestHeader(value = TOKEN_HEADER, required = false) String token) {
        List<SessionEvent> events = bus.drainEvents(sessionId, token);
        return new EventsResponse(events.stream().map(ApiDtos.EventDto::from).toList());
    }

    /** 断线：订阅与确认位点保留，发布/拉取立即被挡。 */
    @PostMapping("/sessions/{sessionId}/disconnect")
    public void disconnect(@PathVariable String sessionId,
                           @RequestHeader(value = TOKEN_HEADER, required = false) String token) {
        bus.disconnect(sessionId, token);
    }

    /** 重连：从已确认位点续传，被摘除的订阅重新挂上。 */
    @PostMapping("/sessions/{sessionId}/reconnect")
    public void reconnect(@PathVariable String sessionId,
                          @RequestHeader(value = TOKEN_HEADER, required = false) String token) {
        bus.reconnect(sessionId, token);
    }

    /** 关闭会话：关闭动作成功返回 204；会话作废、不可再重连，并产生 SESSION_CLOSED 事件。 */
    @DeleteMapping("/sessions/{sessionId}")
    public org.springframework.http.ResponseEntity<Void> close(
            @PathVariable String sessionId,
            @RequestHeader(value = TOKEN_HEADER, required = false) String token) {
        bus.closeSession(sessionId, token);
        return org.springframework.http.ResponseEntity.noContent().build();
    }

    private static byte[] decodePayload(String base64) {
        requireBody(base64 != null && !base64.isBlank(), "发布请求缺少 payloadBase64");
        try {
            byte[] payload = Base64.getDecoder().decode(base64);
            requireBody(payload.length > 0, "空报文");
            return payload;
        } catch (IllegalArgumentException ex) {
            throw new BusException(ErrorCode.MALFORMED_MESSAGE,
                    "payloadBase64 不是合法 Base64");
        }
    }

    private static void requireBody(boolean condition, String message) {
        if (!condition) {
            throw new BusException(ErrorCode.MALFORMED_MESSAGE, message);
        }
    }
}
