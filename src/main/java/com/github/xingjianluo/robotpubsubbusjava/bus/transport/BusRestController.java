package com.github.xingjianluo.robotpubsubbusjava.bus.transport;

import com.github.xingjianluo.robotpubsubbusjava.bus.api.BusSession;
import com.github.xingjianluo.robotpubsubbusjava.bus.api.MessageBus;
import com.github.xingjianluo.robotpubsubbusjava.bus.auth.AuthorizationService;
import com.github.xingjianluo.robotpubsubbusjava.bus.engine.LocalMessageBus;
import com.github.xingjianluo.robotpubsubbusjava.bus.error.BusErrorCode;
import com.github.xingjianluo.robotpubsubbusjava.bus.error.BusException;
import com.github.xingjianluo.robotpubsubbusjava.bus.model.Principal;
import com.github.xingjianluo.robotpubsubbusjava.bus.model.PublishResult;
import com.github.xingjianluo.robotpubsubbusjava.bus.model.ResumeToken;
import com.github.xingjianluo.robotpubsubbusjava.bus.model.StartPosition;
import com.github.xingjianluo.robotpubsubbusjava.bus.model.SubscribeResult;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 最小 HTTP 传输层。
 *
 * <p>身份以 X-Bus-Token 请求头携带，经服务端确认后执行主题级授权；
 * 所有失败转换为带 errorCode 的 JSON 响应，错误码彼此可区分，不静默断开/丢消息。
 * 有序投递、ACK 去重、超时重投、背压策略等语义全部由进程内总线保证。
 */
@RestController
@RequestMapping("/bus")
public class BusRestController {

    private final LocalMessageBus bus;
    private final AuthorizationService auth;

    public BusRestController(MessageBus bus, AuthorizationService auth) {
        // 需要 sessionHandle 传输层辅助方法，故类型为 LocalMessageBus
        this.bus = (LocalMessageBus) bus;
        this.auth = auth;
    }

    private Principal requirePrincipal(String token) {
        Principal principal = auth.authenticate(token);
        if (principal == null) {
            throw new BusException(BusErrorCode.UNAUTHENTICATED,
                    "缺少有效 X-Bus-Token，会话身份未被服务端确认");
        }
        return principal;
    }

    @PostMapping("/publish")
    public PublishResult publish(@RequestHeader(value = "X-Bus-Token", required = false) String token,
                                 @RequestParam String topic,
                                 @RequestBody(required = false) byte[] payload) {
        Principal principal = requirePrincipal(token);
        return bus.publish(principal, topic, payload == null ? new byte[0] : payload);
    }

    @PostMapping("/sessions")
    public Map<String, Object> connect(
            @RequestHeader(value = "X-Bus-Token", required = false) String token,
            @RequestParam(value = "resumeToken", required = false) String resumeToken) {
        Principal principal = requirePrincipal(token);
        BusSession session = bus.connect(principal, resumeToken);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("sessionId", session.sessionId());
        body.put("principal", principal.name());
        body.put("resumeToken", session.resumeToken().encode());
        return body;
    }

    @PostMapping("/subscriptions")
    public SubscribeResult subscribe(@RequestHeader(value = "X-Bus-Token", required = false) String token,
                                     @RequestParam String sessionId,
                                     @RequestParam String topic,
                                     @RequestParam(defaultValue = "EARLIEST") String position) {
        Principal principal = requirePrincipal(token);
        BusSession session = bus.sessionHandle(sessionId, principal);
        StartPosition start;
        try {
            start = StartPosition.valueOf(position.trim().toUpperCase());
        } catch (IllegalArgumentException ex) {
            throw new BusException(BusErrorCode.MALFORMED_MESSAGE,
                    "无法识别的起始位置: " + position + "（应为 EARLIEST 或 LATEST）");
        }
        return session.subscribe(topic, start);
    }

    @PostMapping("/ack")
    public Map<String, Object> ack(@RequestHeader(value = "X-Bus-Token", required = false) String token,
                                   @RequestParam String sessionId,
                                   @RequestParam String topic,
                                   @RequestParam long seq) {
        Principal principal = requirePrincipal(token);
        BusSession session = bus.sessionHandle(sessionId, principal);
        session.ack(topic, seq);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("sessionId", sessionId);
        body.put("topic", topic);
        body.put("committedSeq", seq);
        return body;
    }

    @PostMapping("/sessions/disconnect")
    public Map<String, Object> disconnect(
            @RequestHeader(value = "X-Bus-Token", required = false) String token,
            @RequestParam String sessionId) {
        Principal principal = requirePrincipal(token);
        BusSession session = bus.sessionHandle(sessionId, principal);
        session.disconnect();
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("sessionId", sessionId);
        body.put("resumeToken", session.resumeToken().encode());
        body.put("state", "disconnected");
        return body;
    }

    @GetMapping("/topics")
    public Object topics() {
        return bus.topics().stream().map(t -> {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("name", t.name());
            m.put("backpressurePolicy", t.backpressurePolicy().name());
            m.put("maxInflight", t.maxInflight());
            m.put("retentionSize", t.retentionSize());
            m.put("maxPayloadBytes", t.maxPayloadBytes());
            return m;
        }).toList();
    }

    /** 统一失败语义：错误码 + 可读信息 + HTTP 状态，全部对客户端可见。 */
    @ExceptionHandler(BusException.class)
    public ResponseEntity<Map<String, Object>> handleBusException(BusException ex) {
        BusErrorCode code = ex.errorCode();
        HttpStatus http = switch (code) {
            case UNAUTHENTICATED, RESUME_FORBIDDEN, PUBLISH_FORBIDDEN, SUBSCRIBE_FORBIDDEN,
                 SESSION_CONFLICT -> HttpStatus.FORBIDDEN;
            case UNKNOWN_TOPIC, SUBSCRIPTION_NOT_FOUND, SESSION_CLOSED -> HttpStatus.NOT_FOUND;
            case MALFORMED_MESSAGE, PAYLOAD_TOO_LARGE -> HttpStatus.BAD_REQUEST;
            case SUBSCRIBER_BACKPRESSURE_REJECTED, SLOW_CONSUMER_DISCONNECTED ->
                    HttpStatus.SERVICE_UNAVAILABLE;
            case BUS_CLOSED -> HttpStatus.SERVICE_UNAVAILABLE;
            case INTERNAL_ERROR -> HttpStatus.INTERNAL_SERVER_ERROR;
        };
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("errorCode", code.name());
        body.put("description", code.description());
        body.put("message", ex.getMessage());
        return ResponseEntity.status(http).body(body);
    }
}
