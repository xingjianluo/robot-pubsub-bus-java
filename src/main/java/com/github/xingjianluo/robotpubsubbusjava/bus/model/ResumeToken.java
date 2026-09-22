package com.github.xingjianluo.robotpubsubbusjava.bus.model;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Optional;

/**
 * 断线续传令牌：编码 sessionId + 身份名，重连时由服务端校验，
 * 防止会话被其他身份接管（越权区分）。令牌为不透明字符串，客户端不应解析其内容。
 */
public record ResumeToken(String sessionId, String principalName) {

    private static final Base64.Encoder ENCODER = Base64.getUrlEncoder().withoutPadding();
    private static final Base64.Decoder DECODER = Base64.getUrlDecoder();

    public String encode() {
        String raw = sessionId + "|" + principalName;
        return ENCODER.encodeToString(raw.getBytes(StandardCharsets.UTF_8));
    }

    public static Optional<ResumeToken> decode(String raw) {
        if (raw == null || raw.isBlank()) {
            return Optional.empty();
        }
        try {
            String decoded = new String(DECODER.decode(raw), StandardCharsets.UTF_8);
            int sep = decoded.indexOf('|');
            if (sep <= 0 || sep == decoded.length() - 1) {
                return Optional.empty();
            }
            return Optional.of(new ResumeToken(decoded.substring(0, sep), decoded.substring(sep + 1)));
        } catch (IllegalArgumentException ex) {
            // 非 base64 / 非法字节 -> 令牌畸形
            return Optional.empty();
        }
    }
}
