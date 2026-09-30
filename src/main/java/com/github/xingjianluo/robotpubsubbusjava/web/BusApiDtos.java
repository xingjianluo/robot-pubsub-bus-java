package com.github.xingjianluo.robotpubsubbusjava.web;

import com.github.xingjianluo.robotpubsubbusjava.bus.BusEvent;
import com.github.xingjianluo.robotpubsubbusjava.bus.Delivery;
import com.github.xingjianluo.robotpubsubbusjava.bus.EventKind;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

/** 对外 HTTP 入口的请求/响应报文。负载以 Base64 编码传输。 */
final class BusApiDtos {

    private BusApiDtos() {
    }

    record OpenSessionRequest(String credential) {
    }

    record OpenSessionResponse(String sessionId, String clientId) {
    }

    record TopicsResponse(List<String> topics) {
    }

    record SubscribeRequest(String topic, String policy) {
    }

    record PublishRequest(String topic, String payloadBase64) {
    }

    record PublishResponse(String topic, long seq) {
    }

    record PollRequest(String topic, Integer max, Long waitMillis) {
    }

    record DeliveryView(String topic, long seq, String payloadBase64,
                        boolean redelivered, int attempt) {

        static DeliveryView of(Delivery delivery) {
            return new DeliveryView(delivery.message().topic(), delivery.message().seq(),
                    Base64.getEncoder().encodeToString(delivery.message().payload()),
                    delivery.redelivered(), delivery.attempt());
        }
    }

    record EventView(String sessionId, String topic, EventKind kind,
                     long fromSeq, long toSeq, List<Long> missingSeqs, String detail) {

        static EventView of(BusEvent event) {
            return new EventView(event.sessionId(), event.topic(), event.kind(),
                    event.fromSeq(), event.toSeq(), event.missingSeqs(), event.detail());
        }
    }

    record PollResponse(List<DeliveryView> deliveries, List<EventView> events) {
    }

    record AckRequest(String topic, long seq) {
    }

    record EventsResponse(List<EventView> events) {
    }

    record ErrorResponse(String code, String message) {
    }

    static byte[] decodePayload(String payloadBase64) {
        if (payloadBase64 == null) {
            return null;
        }
        return Base64.getDecoder().decode(payloadBase64);
    }

    static String utf8(byte[] payload) {
        return new String(payload, StandardCharsets.UTF_8);
    }
}
