package com.github.xingjianluo.robotpubsubbusjava.web;

import com.github.xingjianluo.robotpubsubbusjava.bus.BackpressurePolicy;
import com.github.xingjianluo.robotpubsubbusjava.bus.BusException;
import com.github.xingjianluo.robotpubsubbusjava.bus.ErrorCode;
import com.github.xingjianluo.robotpubsubbusjava.bus.MessageBus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * 消息总线对外 HTTP 入口：客户端通过网络完成建会话、订阅、发布、
 * 拉取、确认、断线/重连/关闭与主题清单查看。
 *
 * <p>会话身份只在建会话时由服务端核验（凭据 → 服务端确认的身份），
 * 之后所有操作以会话标识进行，权限以服务端认下的身份为准。
 */
@RestController
@RequestMapping("/bus")
public class BusController {

    private final MessageBus bus;

    public BusController(MessageBus bus) {
        this.bus = bus;
    }

    /** 建会话：凭据由服务端核验；未携带/非法凭据直接拒绝。 */
    @PostMapping("/sessions")
    public BusApiDtos.OpenSessionResponse openSession(
            @RequestBody(required = false) BusApiDtos.OpenSessionRequest request) {
        String credential = request == null ? null : request.credential();
        String sessionId = bus.openSession(credential);
        return new BusApiDtos.OpenSessionResponse(sessionId, bus.clientId(sessionId));
    }

    /** 查看主题清单。 */
    @GetMapping("/topics")
    public BusApiDtos.TopicsResponse topics() {
        return new BusApiDtos.TopicsResponse(bus.listTopics());
    }

    /** 订阅主题，可按订阅选择背压策略（DROP_OLDEST / EVICT_SUBSCRIPTION）。 */
    @PostMapping("/sessions/{sessionId}/subscriptions")
    public void subscribe(@PathVariable String sessionId,
                          @RequestBody BusApiDtos.SubscribeRequest request) {
        bus.subscribe(sessionId, request.topic(), parsePolicy(request.policy()));
    }

    /** 发布消息，返回主题内序号。 */
    @PostMapping("/sessions/{sessionId}/publications")
    public BusApiDtos.PublishResponse publish(@PathVariable String sessionId,
                                              @RequestBody BusApiDtos.PublishRequest request) {
        long seq = bus.publish(sessionId, request.topic(),
                BusApiDtos.decodePayload(request.payloadBase64()));
        return new BusApiDtos.PublishResponse(request.topic(), seq);
    }

    /** 拉取投递：返回本次投递与自上次以来累积的客户端可见事件。 */
    @PostMapping("/sessions/{sessionId}/poll")
    public BusApiDtos.PollResponse poll(@PathVariable String sessionId,
                                        @RequestBody BusApiDtos.PollRequest request) {
        int max = request.max() == null ? 10 : request.max();
        long waitMillis = request.waitMillis() == null ? 0L : request.waitMillis();
        List<BusApiDtos.DeliveryView> deliveries = bus
                .poll(sessionId, request.topic(), max, waitMillis)
                .stream().map(BusApiDtos.DeliveryView::of).toList();
        List<BusApiDtos.EventView> events = bus.drainEvents(sessionId)
                .stream().map(BusApiDtos.EventView::of).toList();
        return new BusApiDtos.PollResponse(deliveries, events);
    }

    /** 累积确认。 */
    @PostMapping("/sessions/{sessionId}/acks")
    public void ack(@PathVariable String sessionId,
                    @RequestBody BusApiDtos.AckRequest request) {
        bus.ack(sessionId, request.topic(), request.seq());
    }

    /** 断线：此后该会话的发布/拉取被明确挡住，直到重连。 */
    @PostMapping("/sessions/{sessionId}/disconnect")
    public void disconnect(@PathVariable String sessionId) {
        bus.disconnect(sessionId);
    }

    /** 重连：从已确认位点续传，被摘除的订阅随重连复活。 */
    @PostMapping("/sessions/{sessionId}/reconnect")
    public void reconnect(@PathVariable String sessionId) {
        bus.reconnect(sessionId);
    }

    /** 关闭会话（终态）。 */
    @PostMapping("/sessions/{sessionId}/close")
    public void close(@PathVariable String sessionId) {
        bus.closeSession(sessionId);
    }

    /** 读取并清空该会话的客户端可见事件。 */
    @GetMapping("/sessions/{sessionId}/events")
    public BusApiDtos.EventsResponse events(@PathVariable String sessionId) {
        return new BusApiDtos.EventsResponse(bus.drainEvents(sessionId)
                .stream().map(BusApiDtos.EventView::of).toList());
    }

    private static BackpressurePolicy parsePolicy(String policy) {
        if (policy == null || policy.isBlank()) {
            return BackpressurePolicy.DROP_OLDEST;
        }
        try {
            return BackpressurePolicy.valueOf(policy.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new BusException(ErrorCode.MALFORMED_MESSAGE,
                    "未知背压策略 " + policy + "，可选 DROP_OLDEST / EVICT_SUBSCRIPTION");
        }
    }
}
