package com.github.xingjianluo.robotpubsubbusjava.bus.api;

import com.github.xingjianluo.robotpubsubbusjava.bus.model.Principal;
import com.github.xingjianluo.robotpubsubbusjava.bus.model.PublishResult;
import com.github.xingjianluo.robotpubsubbusjava.bus.model.ResumeToken;
import com.github.xingjianluo.robotpubsubbusjava.bus.model.SessionEvent;
import com.github.xingjianluo.robotpubsubbusjava.bus.model.StartPosition;
import com.github.xingjianluo.robotpubsubbusjava.bus.model.SubscribeResult;

/**
 * 一条客户端会话：可在多个主题上订阅，通过事件流接收投递/丢弃/缺口/断开事件。
 */
public interface BusSession {

    String sessionId();

    Principal principal();

    ResumeToken resumeToken();

    /** 订阅主题；重连恢复后再订阅同一主题返回既有位点。 */
    SubscribeResult subscribe(String topic, StartPosition startPosition);

    /** 确认某主题上 <= seq 的消息，推进去重位点（位点之后的消息不会重复交付）。 */
    void ack(String topic, long seq);

    /** 会话内发布（身份仍由服务端授权）。 */
    PublishResult publish(String topic, byte[] payload);

    /** 阻塞拉取下一个事件；超时返回 null。 */
    SessionEvent poll(long timeoutMillis) throws InterruptedException;

    /** 主动断开；会话与位点保留以便续传。 */
    void disconnect();

    boolean closed();
}
