package com.github.xingjianluo.robotpubsubbusjava.bus;

import java.util.Map;
import java.util.Objects;

/**
 * 基于预置令牌表的 {@link SessionAuthenticator}：令牌到身份的映射由服务端配置，
 * 客户端只持有令牌，身份与权限始终由服务端认下。适用于本地部署与测试，
 * 不依赖外部认证服务、不使用真实凭据。
 */
public final class StaticTokenAuthenticator implements SessionAuthenticator {

    private final Map<String, SessionIdentity> identitiesByToken;

    public StaticTokenAuthenticator(Map<String, SessionIdentity> identitiesByToken) {
        this.identitiesByToken = Map.copyOf(identitiesByToken);
    }

    @Override
    public SessionIdentity authenticate(String credential) {
        if (credential == null || credential.isBlank()) {
            throw new BusException(ErrorCode.UNAUTHENTICATED, "建会话请求未携带身份凭据");
        }
        SessionIdentity identity = identitiesByToken.get(credential);
        if (identity == null) {
            throw new BusException(ErrorCode.AUTHENTICATION_FAILED, "身份凭据不被服务端认可");
        }
        return Objects.requireNonNull(identity);
    }
}
