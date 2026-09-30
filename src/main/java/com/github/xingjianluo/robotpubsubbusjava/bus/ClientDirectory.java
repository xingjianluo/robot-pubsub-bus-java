package com.github.xingjianluo.robotpubsubbusjava.bus;

import java.util.Optional;

/**
 * 客户端凭据登记册：建会话时凭令牌在此核验并取回服务端登记的身份材料。
 * 实现必须线程安全。
 */
public interface ClientDirectory {

    /**
     * 按令牌解析登记客户端。
     *
     * @param token 客户端令牌
     * @return 登记客户端；令牌为空或不存在时返回 empty，由上层区分“未携带”与“不合法”
     */
    Optional<RegisteredClient> resolve(String token);
}
