package com.github.xingjianluo.robotpubsubbusjava.bus.error;

/**
 * 总线上所有失败的统一错误码，保证各类失败彼此可区分、对客户端可见。
 */
public enum BusErrorCode {
    // 报文类
    MALFORMED_MESSAGE("报文畸形，无法解析"),
    PAYLOAD_TOO_LARGE("报文超出主题字节上限"),
    // 主题类
    UNKNOWN_TOPIC("主题不存在"),
    // 授权类
    UNAUTHENTICATED("缺少或携带无效身份令牌"),
    PUBLISH_FORBIDDEN("身份无权向该主题发布"),
    SUBSCRIBE_FORBIDDEN("身份无权订阅该主题"),
    RESUME_FORBIDDEN("续传令牌与会话身份不匹配"),
    SESSION_CONFLICT("会话已被其他连接占用或被接管"),
    // 背压 / 资源类
    SUBSCRIBER_BACKPRESSURE_REJECTED("订阅者积压达上限，按 REJECT 策略拒绝本次发布"),
    SLOW_CONSUMER_DISCONNECTED("订阅者消费过慢，按 DISCONNECT 策略被断开"),
    // 会话状态类
    SESSION_CLOSED("会话已关闭"),
    SUBSCRIPTION_NOT_FOUND("订阅不存在或不属于当前会话"),
    BUS_CLOSED("总线已关闭"),
    // 服务类
    INTERNAL_ERROR("总线内部错误");

    private final String description;

    BusErrorCode(String description) {
        this.description = description;
    }

    public String description() {
        return description;
    }
}
