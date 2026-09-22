package com.github.xingjianluo.robotpubsubbusjava.bus.auth;

import com.github.xingjianluo.robotpubsubbusjava.bus.config.BusProperties;
import com.github.xingjianluo.robotpubsubbusjava.bus.model.Principal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** 基于本地配置（application.properties）的身份与授权实现，无外部依赖。 */
public class ConfigAuthorizationService implements AuthorizationService {

    private final BusProperties properties;
    private final Map<String, Principal> tokenIndex = new ConcurrentHashMap<>();

    public ConfigAuthorizationService(BusProperties properties) {
        this.properties = properties;
        rebuild();
    }

    private void rebuild() {
        Map<String, Principal> built = new LinkedHashMap<>();
        properties.getPrincipals().forEach((token, props) -> {
            String name = props.getName() != null ? props.getName() : token;
            Set<String> publish = Set.copyOf(nonNull(props.getPublish()));
            Set<String> subscribe = Set.copyOf(nonNull(props.getSubscribe()));
            built.put(token, new Principal(name, token, publish, subscribe));
        });
        tokenIndex.clear();
        tokenIndex.putAll(built);
    }

    private static List<String> nonNull(List<String> values) {
        return values == null ? List.of() : values;
    }

    @Override
    public Principal authenticate(String token) {
        if (token == null || token.isBlank()) {
            return null;
        }
        return tokenIndex.get(token);
    }

    @Override
    public boolean canPublish(Principal principal, String topic) {
        return matches(principal.allowedPublishTopics(), topic);
    }

    @Override
    public boolean canSubscribe(Principal principal, String topic) {
        return matches(principal.allowedSubscribeTopics(), topic);
    }

    /**
     * 匹配规则：精确匹配；"*" 匹配任意主题；"prefix.*" 匹配前缀下的主题段。
     */
    static boolean matches(Set<String> patterns, String topic) {
        if (patterns == null || topic == null) {
            return false;
        }
        for (String pattern : patterns) {
            if ("*".equals(pattern) || pattern.equals(topic)) {
                return true;
            }
            if (pattern.endsWith(".*")) {
                String prefix = pattern.substring(0, pattern.length() - 1); // 保留结尾 '.'
                if (topic.startsWith(prefix)) {
                    return true;
                }
            }
        }
        return false;
    }
}
