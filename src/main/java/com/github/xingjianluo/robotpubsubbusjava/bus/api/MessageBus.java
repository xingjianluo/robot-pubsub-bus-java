package com.github.xingjianluo.robotpubsubbusjava.bus.api;

import com.github.xingjianluo.robotpubsubbusjava.bus.model.Principal;
import com.github.xingjianluo.robotpubsubbusjava.bus.model.PublishResult;
import com.github.xingjianluo.robotpubsubbusjava.bus.model.StartPosition;
import com.github.xingjianluo.robotpubsubbusjava.bus.model.TopicConfig;
import java.util.Collection;

/**
 * 本地发布/订阅消息总线。会话身份由服务端确认；会话可断开/重连续传。
 */
public interface MessageBus {

    /** 建立会话；resumeToken 非空时尝试恢复既有会话与全部订阅位点。 */
    BusSession connect(Principal principal, String resumeToken);

    /** 向主题发布消息（主题内全局有序）。 */
    PublishResult publish(Principal principal, String topic, byte[] payload);

    /** 主题清单与配置。 */
    Collection<TopicConfig> topics();

    /** 关闭总线，释放后台线程。 */
    void close();
}
