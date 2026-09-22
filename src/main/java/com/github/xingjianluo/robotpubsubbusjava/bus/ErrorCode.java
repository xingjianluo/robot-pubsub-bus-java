package com.github.xingjianluo.robotpubsubbusjava.bus;

/**
 * 总线统一失败语义：所有失败都以错误码形式对客户端可见且彼此可区分，
 * 不允许静默断开或静默丢消息。
 */
public enum ErrorCode {
    /** 主题未在服务端注册 */
    UNKNOWN_TOPIC,
    /** 会话身份未被授权向该主题发布 */
    UNAUTHORIZED_PUBLISH,
    /** 会话身份未被授权订阅该主题 */
    UNAUTHORIZED_SUBSCRIBE,
    /** 报文为空或结构非法 */
    MALFORMED_MESSAGE,
    /** 报文超过最大负载限制 */
    MESSAGE_TOO_LARGE,
    /** 会话不存在（例如重连时携带了未知会话标识） */
    SESSION_NOT_FOUND,
    /** 订阅不存在 */
    SUBSCRIPTION_NOT_FOUND,
    /** 订阅已因慢消费者策略被断开 */
    SUBSCRIPTION_EVICTED
}
