package com.github.xingjianluo.robotpubsubbusjava.bus;

/**
 * 会话身份核验器：建会话时由服务端调用，把客户端凭据核验为
 * 服务端认可的 {@link SessionIdentity}。客户端不能自称身份。
 */
public interface SessionAuthenticator {

    /**
     * 核验凭据并返回服务端确认的身份。
     *
     * @param credential 客户端凭据；{@code null} 或空串表示未携带
     * @throws BusException 未携带凭据（{@link ErrorCode#UNAUTHENTICATED}）
     *                      或凭据非法（{@link ErrorCode#AUTHENTICATION_FAILED}）
     */
    SessionIdentity authenticate(String credential);
}
