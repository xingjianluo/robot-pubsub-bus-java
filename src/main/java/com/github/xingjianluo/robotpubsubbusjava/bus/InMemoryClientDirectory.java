package com.github.xingjianluo.robotpubsubbusjava.bus;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 基于内存的客户端凭据登记册，适用于本地单机验证与测试，不依赖外部鉴权服务。
 * 重复登记同一令牌时后登记的覆盖先登记的。
 */
public final class InMemoryClientDirectory implements ClientDirectory {

    private final Map<String, RegisteredClient> clients = new ConcurrentHashMap<>();

    public InMemoryClientDirectory() {
    }

    public InMemoryClientDirectory(List<RegisteredClient> seed) {
        if (seed != null) {
            seed.forEach(this::register);
        }
    }

    /** 登记（或覆盖）一个客户端。 */
    public void register(RegisteredClient client) {
        clients.put(client.token(), client);
    }

    @Override
    public Optional<RegisteredClient> resolve(String token) {
        if (token == null || token.isBlank()) {
            return Optional.empty();
        }
        return Optional.ofNullable(clients.get(token));
    }
}
