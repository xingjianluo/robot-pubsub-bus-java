package com.github.xingjianluo.robotpubsubbusjava.bus;

/**
 * 总线统一失败语义：所有失败都以错误码形式对客户端可见且彼此可区分，
 * 不允许静默断开或静默丢消息。
 */
public enum ErrorCode {
    /** 请求未携带身份凭据（如 X-Client-Token 缺失） */
    UNAUTHENTICATED,
    /** 凭据无效或与会话归属不匹配，服务端无法核验会话身份 */
    INVALID_CREDENTIALS,
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
    /** 会话处于断线状态，该操作要求会话在线 */
    SESSION_DISCONNECTED,
    /** 会话已被永久关闭，不可再重连 */
    SESSION_CLOSED,
    /** 订阅不存在 */
    SUBSCRIPTION_NOT_FOUND,
    /** 订阅已因背压策略被摘除；会话保留，重连后可从确认位点续收 */
    SUBSCRIPTION_EVICTED
}
