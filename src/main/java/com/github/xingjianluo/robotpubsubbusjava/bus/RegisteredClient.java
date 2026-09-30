package com.github.xingjianluo.robotpubsubbusjava.bus;

import java.util.Set;

/**
 * 服务端登记的客户端凭据。客户端只能凭登记过的令牌建会话，会话身份由此服务端材料推导，
 * 客户端不能自称是谁就拿到谁的发布/订阅权限。
 *
 * @param token            客户端令牌（建会话请求携带，服务端核验）
 * @param clientId         服务端登记的客户端标识
 * @param publishTopics    授权发布的主题集合，"*" 表示全部
 * @param subscribeTopics  授权订阅的主题集合，"*" 表示全部
 */
public record RegisteredClient(String token,
                               String clientId,
                               Set<String> publishTopics,
                               Set<String> subscribeTopics) {

    public RegisteredClient {
        if (token == null || token.isBlank()) {
            throw new IllegalArgumentException("客户端令牌不能为空");
        }
        if (clientId == null || clientId.isBlank()) {
            throw new IllegalArgumentException("客户端标识不能为空");
        }
        publishTopics = publishTopics == null ? Set.of() : Set.copyOf(publishTopics);
        subscribeTopics = subscribeTopics == null ? Set.of() : Set.copyOf(subscribeTopics);
    }

    /** 由登记材料构造服务端确认的会话身份。 */
    public SessionIdentity toIdentity() {
        return new SessionIdentity(clientId, publishTopics, subscribeTopics);
    }
}
