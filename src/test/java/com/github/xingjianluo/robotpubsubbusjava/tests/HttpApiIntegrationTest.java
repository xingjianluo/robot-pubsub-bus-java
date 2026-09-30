package com.github.xingjianluo.robotpubsubbusjava.tests;

import com.github.xingjianluo.robotpubsubbusjava.bus.BackpressureStrategy;
import com.github.xingjianluo.robotpubsubbusjava.web.ApiDtos.AckRequest;
import com.github.xingjianluo.robotpubsubbusjava.web.ApiDtos.DeliveryDto;
import com.github.xingjianluo.robotpubsubbusjava.web.ApiDtos.EventDto;
import com.github.xingjianluo.robotpubsubbusjava.web.ApiDtos.EventsResponse;
import com.github.xingjianluo.robotpubsubbusjava.web.ApiDtos.OpenSessionResponse;
import com.github.xingjianluo.robotpubsubbusjava.web.ApiDtos.PollResponse;
import com.github.xingjianluo.robotpubsubbusjava.web.ApiDtos.PublishRequest;
import com.github.xingjianluo.robotpubsubbusjava.web.ApiDtos.PublishResponse;
import com.github.xingjianluo.robotpubsubbusjava.web.ApiDtos.SubscribeRequest;
import com.github.xingjianluo.robotpubsubbusjava.web.ApiDtos.TopicsResponse;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.annotation.DirtiesContext;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 总线对外 HTTP 入口的端到端验证：真实本地端口，不依赖外部服务与真实凭据。
 * 覆盖建会话鉴权、越权/未知主题/畸形错误区分、订阅发布拉取确认全链路、
 * 断线拦截与重连续传、事件分类、主题清单、订阅摘除后会话保留。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "spring.profiles.active=integration")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class HttpApiIntegrationTest {

    private static final String COMMANDS = "robot.commands";
    private static final String COMMANDER = "commander-token";
    private static final String OBSERVER = "observer-token";

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate rest;

    /** 统一的原始响应：状态码 + 错误码（正常响应时 error 为 null）。 */
    record Raw(int status, String error, Map<String, Object> body) {
    }

    private String url(String path) {
        return "http://localhost:" + port + path;
    }

    private HttpHeaders headers(String token) {
        HttpHeaders h = new HttpHeaders();
        if (token != null) {
            h.set("X-Client-Token", token);
        }
        return h;
    }

    private static String b64(String text) {
        return Base64.getEncoder().encodeToString(text.getBytes(StandardCharsets.UTF_8));
    }

    @SuppressWarnings("unchecked")
    private Raw exchange(String path, HttpMethod method, String token, Object body) {
        ResponseEntity<Map> resp = rest.exchange(url(path), method,
                new HttpEntity<>(body, headers(token)), Map.class);
        Map<String, Object> b = resp.getBody();
        String error = b == null ? null : (String) b.get("error");
        return new Raw(resp.getStatusCode().value(), error, b);
    }

    private String openSession(String token) {
        ResponseEntity<OpenSessionResponse> resp = rest.exchange(
                url("/api/sessions"), HttpMethod.POST,
                new HttpEntity<>(headers(token)), OpenSessionResponse.class);
        assertEquals(HttpStatus.OK, resp.getStatusCode());
        return resp.getBody().sessionId();
    }

    private long publish(String session, String token, String topic, String text) {
        ResponseEntity<PublishResponse> resp = rest.postForEntity(url(messages(session)),
                new HttpEntity<>(new PublishRequest(topic, b64(text)), headers(token)),
                PublishResponse.class);
        assertEquals(HttpStatus.OK, resp.getStatusCode(), "发布失败: " + resp.getStatusCode());
        return resp.getBody().seq();
    }

    private PollResponse poll(String session, String token, String topic) {
        ResponseEntity<PollResponse> resp = rest.exchange(
                url(pollPath(session, topic)), HttpMethod.GET,
                new HttpEntity<>(headers(token)), PollResponse.class);
        assertEquals(HttpStatus.OK, resp.getStatusCode());
        return resp.getBody();
    }

    private String subs(String session) {
        return "/api/sessions/" + session + "/subscriptions";
    }

    private String messages(String session) {
        return "/api/sessions/" + session + "/messages";
    }

    private String pollPath(String session, String topic) {
        return "/api/sessions/" + session + "/subscriptions/" + topic + "/messages?max=100";
    }

    @Test
    void authenticationRejectsMissingAndBadTokens() {
        Raw noToken = exchange("/api/sessions", HttpMethod.POST, null, null);
        assertEquals(401, noToken.status());
        assertEquals("UNAUTHENTICATED", noToken.error());

        Raw badToken = exchange("/api/sessions", HttpMethod.POST, "nonsense", null);
        assertEquals(403, badToken.status());
        assertEquals("INVALID_CREDENTIALS", badToken.error());

        ResponseEntity<OpenSessionResponse> ok = rest.exchange(
                url("/api/sessions"), HttpMethod.POST,
                new HttpEntity<>(headers(COMMANDER)), OpenSessionResponse.class);
        assertEquals(HttpStatus.OK, ok.getStatusCode());
        assertEquals("commander", ok.getBody().clientId());
    }

    @Test
    void fullChainOverHttpWithAuthzDisconnectAndResume() {
        String commander = openSession(COMMANDER);
        String observer = openSessionToken(OBSERVER, "observer");

        // 主题清单
        ResponseEntity<TopicsResponse> topics = rest.getForEntity(url("/api/topics"),
                TopicsResponse.class);
        assertEquals(HttpStatus.OK, topics.getStatusCode());
        assertEquals(List.of("robot.commands", "robot.telemetry"), topics.getBody().topics());

        // observer 订阅指令主题（默认 DROP_OLDEST）
        Raw sub = exchange(subs(observer), HttpMethod.POST, OBSERVER,
                new SubscribeRequest(COMMANDS, null));
        assertEquals(200, sub.status());

        // observer 无权发布指令主题：403 UNAUTHORIZED_PUBLISH（与未鉴权不同）
        Raw forbidden = exchange(messages(observer), HttpMethod.POST, OBSERVER,
                new PublishRequest(COMMANDS, b64("x")));
        assertEquals(403, forbidden.status());
        assertEquals("UNAUTHORIZED_PUBLISH", forbidden.error());

        // commander 无权订阅指令主题：403 UNAUTHORIZED_SUBSCRIBE
        Raw forbiddenSub = exchange(subs(commander), HttpMethod.POST, COMMANDER,
                new SubscribeRequest(COMMANDS, null));
        assertEquals(403, forbiddenSub.status());
        assertEquals("UNAUTHORIZED_SUBSCRIBE", forbiddenSub.error());

        // 未知主题：404 UNKNOWN_TOPIC
        Raw unknown = exchange(messages(commander), HttpMethod.POST, COMMANDER,
                new PublishRequest("robot.nope", b64("x")));
        assertEquals(404, unknown.status());
        assertEquals("UNKNOWN_TOPIC", unknown.error());

        // 令牌与会话归属不匹配：403 INVALID_CREDENTIALS
        Raw wrongToken = exchange(messages(observer), HttpMethod.POST, COMMANDER,
                new PublishRequest(COMMANDS, b64("x")));
        assertEquals(403, wrongToken.status());
        assertEquals("INVALID_CREDENTIALS", wrongToken.error());

        // 正常发布两条
        assertEquals(1L, publish(commander, COMMANDER, COMMANDS, "cmd-1"));
        assertEquals(2L, publish(commander, COMMANDER, COMMANDS, "cmd-2"));

        // observer 拉取（首投 redelivered=false, attempt=1）
        PollResponse polled = poll(observer, OBSERVER, COMMANDS);
        assertEquals(2, polled.deliveries().size());
        DeliveryDto d1 = polled.deliveries().get(0);
        assertEquals(1L, d1.seq());
        assertFalse(d1.redelivered());
        assertEquals(1, d1.attempt());
        assertArrayEquals("cmd-1".getBytes(StandardCharsets.UTF_8),
                Base64.getDecoder().decode(d1.payloadBase64()));

        // 累积确认到 1
        Raw ack = exchange("/api/sessions/" + observer + "/acks", HttpMethod.POST, OBSERVER,
                new AckRequest(COMMANDS, 1L));
        assertEquals(200, ack.status());

        // 断线后拉取被明确挡住：409 SESSION_DISCONNECTED，绝不能再拉到消息
        assertEquals(200, exchange("/api/sessions/" + observer + "/disconnect",
                HttpMethod.POST, OBSERVER, null).status());
        Raw blocked = exchange(pollPath(observer, COMMANDS), HttpMethod.GET, OBSERVER, null);
        assertEquals(409, blocked.status());
        assertEquals("SESSION_DISCONNECTED", blocked.error());

        // 断线期间 commander 继续发布
        publish(commander, COMMANDER, COMMANDS, "cmd-3");

        // 重连后从确认位点续传：seq=1 不重复，seq=2 重投，seq=3 首投
        assertEquals(200, exchange("/api/sessions/" + observer + "/reconnect",
                HttpMethod.POST, OBSERVER, null).status());
        PollResponse resumed = poll(observer, OBSERVER, COMMANDS);
        List<Long> seqs = resumed.deliveries().stream().map(DeliveryDto::seq).toList();
        assertEquals(List.of(2L, 3L), seqs, "判定依据：已确认 seq=1 不重复，断线缓冲 seq=3 可收");
        assertTrue(resumed.deliveries().get(0).redelivered(), "判定依据：未确认 seq=2 重投");
        assertFalse(resumed.deliveries().get(1).redelivered());
    }

    @Test
    void evictedSubscriptionKeepsSessionAndReportsDistinctEvents() {
        String commander = openSession(COMMANDER);
        String observer = openSessionToken(OBSERVER, "observer");
        Raw sub = exchange(subs(observer), HttpMethod.POST, OBSERVER,
                new SubscribeRequest(COMMANDS, BackpressureStrategy.EVICT_SUBSCRIPTION));
        assertEquals(200, sub.status());

        for (int i = 1; i <= 4; i++) {
            publish(commander, COMMANDER, COMMANDS, "cmd-" + i);
        }

        Raw pollResp = exchange(pollPath(observer, COMMANDS), HttpMethod.GET, OBSERVER, null);
        assertEquals(409, pollResp.status());
        assertEquals("SUBSCRIPTION_EVICTED", pollResp.error());

        // 会话本身仍在：读事件返回 200
        ResponseEntity<EventsResponse> eventsResp = rest.exchange(
                url("/api/sessions/" + observer + "/events"), HttpMethod.GET,
                new HttpEntity<>(headers(OBSERVER)), EventsResponse.class);
        assertEquals(HttpStatus.OK, eventsResp.getStatusCode());
        List<EventDto> events = eventsResp.getBody().events();
        assertTrue(events.stream().anyMatch(e -> e.type().equals("BACKLOG_GAP")),
                "判定依据：缺口事件必须可见");
        assertTrue(events.stream().anyMatch(e -> e.type().equals("SUBSCRIPTION_EVICTED")),
                "判定依据：摘除事件必须与缺口事件区分");

        // 重连后订阅重挂，会话仍在且可继续收新消息
        assertEquals(200, exchange("/api/sessions/" + observer + "/reconnect",
                HttpMethod.POST, OBSERVER, null).status());
        publish(commander, COMMANDER, COMMANDS, "cmd-5");
        PollResponse resumed = poll(observer, OBSERVER, COMMANDS);
        assertEquals(List.of(5L),
                resumed.deliveries().stream().map(DeliveryDto::seq).toList(),
                "判定依据：重挂后只收重挂之后的新消息");
    }

    @Test
    void closedSessionIsGoneAndReportsSessionClosed() {
        String observer = openSessionToken(OBSERVER, "observer");
        Raw closed = exchange("/api/sessions/" + observer, HttpMethod.DELETE, OBSERVER, null);
        assertEquals(204, closed.status(), "关闭动作本身成功应返回 204");

        ResponseEntity<EventsResponse> events = rest.exchange(
                url("/api/sessions/" + observer + "/events"), HttpMethod.GET,
                new HttpEntity<>(headers(OBSERVER)), EventsResponse.class);
        assertEquals(HttpStatus.OK, events.getStatusCode());
        assertEquals("SESSION_CLOSED", events.getBody().events().get(0).type());

        Raw reconnect = exchange("/api/sessions/" + observer + "/reconnect",
                HttpMethod.POST, OBSERVER, null);
        assertEquals(410, reconnect.status());
        assertEquals("SESSION_CLOSED", reconnect.error());
    }

    /** 建会话并顺带断言服务端认下的 clientId。 */
    private String openSessionToken(String token, String expectedClientId) {
        ResponseEntity<OpenSessionResponse> resp = rest.exchange(
                url("/api/sessions"), HttpMethod.POST,
                new HttpEntity<>(headers(token)), OpenSessionResponse.class);
        assertEquals(HttpStatus.OK, resp.getStatusCode());
        assertEquals(expectedClientId, resp.getBody().clientId());
        return resp.getBody().sessionId();
    }
}
