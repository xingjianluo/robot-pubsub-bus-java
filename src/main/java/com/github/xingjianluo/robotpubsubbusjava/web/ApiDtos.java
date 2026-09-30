package com.github.xingjianluo.robotpubsubbusjava.web;

import com.github.xingjianluo.robotpubsubbusjava.bus.BackpressureStrategy;
import com.github.xingjianluo.robotpubsubbusjava.bus.Delivery;
import com.github.xingjianluo.robotpubsubbusjava.bus.SeqGap;
import com.github.xingjianluo.robotpubsubbusjava.bus.SessionEvent;

import java.util.Base64;
import java.util.List;

/**
 * 总线 HTTP 入口使用的请求/响应 DTO 集合。负载统一以 Base64 传输，避免二进制/空字节问题。
 */
public final class ApiDtos {

    private ApiDtos() {
    }

    /** 订阅请求。strategy 缺省为 DROP_OLDEST。 */
    public record SubscribeRequest(String topic, BackpressureStrategy strategy) {
    }

    /** 发布请求。 */
    public record PublishRequest(String topic, String payloadBase64) {
    }

    /** 确认请求。 */
    public record AckRequest(String topic, long seq) {
    }

    /** 建会话响应。 */
    public record OpenSessionResponse(String sessionId, String clientId) {
    }

    /** 主题清单响应。 */
    public record TopicsResponse(List<String> topics) {
    }

    /** 发布响应。 */
    public record PublishResponse(String topic, long seq) {
    }

    /** 确认响应。 */
    public record AckResponse(String topic, long ackedSeq) {
    }

    /** 单条投递。 */
    public record DeliveryDto(String topic,
                              long seq,
                              String payloadBase64,
                              long publishedAtMillis,
                              boolean redelivered,
                              int attempt) {

        static DeliveryDto from(Delivery delivery) {
            return new DeliveryDto(
                    delivery.message().topic(),
                    delivery.message().seq(),
                    Base64.getEncoder().encodeToString(delivery.message().payload()),
                    delivery.message().publishedAtMillis(),
                    delivery.redelivered(),
                    delivery.attempt());
        }
    }

    /** 序号缺口。 */
    public record GapDto(long fromSeq, long toSeq) {
        static GapDto from(SeqGap gap) {
            return new GapDto(gap.fromSeq(), gap.toSeq());
        }
    }

    /** 会话事件：type 为 SessionEventType 名称。 */
    public record EventDto(String type, String topic, List<GapDto> gaps, String detail) {
        static EventDto from(SessionEvent event) {
            return new EventDto(
                    event.type().name(),
                    event.topic(),
                    event.gaps().stream().map(GapDto::from).toList(),
                    event.detail());
        }
    }

    /** 拉取响应。 */
    public record PollResponse(List<DeliveryDto> deliveries) {
    }

    /** 事件响应：读取并清空当前已产生的事件。 */
    public record EventsResponse(List<EventDto> events) {
    }
}
