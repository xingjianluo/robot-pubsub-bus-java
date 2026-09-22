package com.github.xingjianluo.robotpubsubbusjava.bus.model;

import java.util.Optional;

/**
 * 订阅会话上的事件。投递事件、丢弃通知与缺口通知彼此可区分，
 * 失败不会表现为静默丢消息。
 */
public sealed interface SessionEvent
        permits SessionEvent.Delivery, SessionEvent.Dropped, SessionEvent.Gap, SessionEvent.SessionClosed {

    String subscriptionId();

    /** 消息投递（首次或重投）。deliveryAttempt 用于区分首次/重投。 */
    record Delivery(String subscriptionId, Message message, DeliveryAttempt deliveryAttempt, int attemptCount)
            implements SessionEvent {
    }

    /** DROP_OLDEST 策略生效时，向该订阅者显式通知被丢弃的序号区间。 */
    record Dropped(String subscriptionId, long fromSeq, long toSeq, String reason)
            implements SessionEvent {
    }

    /** 续传时主题保留窗口已覆盖请求位点，无法补齐，显式通知而非静默跳过。 */
    record Gap(String subscriptionId, long expectedSeq, long oldestAvailableSeq)
            implements SessionEvent {
    }

    /** 会话被服务端关闭（慢消费断开、会话冲突等），携带可区分原因。 */
    record SessionClosed(String subscriptionId, String reasonCode, String message)
            implements SessionEvent {
        @Override
        public String subscriptionId() {
            return subscriptionId == null ? "" : subscriptionId;
        }

        public Optional<String> reasonCodeOpt() {
            return Optional.ofNullable(reasonCode);
        }
    }
}
