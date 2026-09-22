package com.github.xingjianluo.robotpubsubbusjava.bus.auth;

import com.github.xingjianluo.robotpubsubbusjava.bus.model.Principal;

/** 会话身份认证与主题级授权。 */
public interface AuthorizationService {

    /** 依据令牌确认会话身份；无效令牌返回空。 */
    Principal authenticate(String token);

    /** 是否允许向主题发布。 */
    boolean canPublish(Principal principal, String topic);

    /** 是否允许订阅主题。 */
    boolean canSubscribe(Principal principal, String topic);
}
