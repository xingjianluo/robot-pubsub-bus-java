package com.github.xingjianluo.robotpubsubbusjava.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * 对外 HTTP 入口端到端：建会话（服务端鉴权）、订阅、发布、拉取、确认、
 * 断线拦截、重连续传、主题清单，以及各类失败的可区分错误码。
 * 使用 application.properties 中的本地开发令牌，不依赖外部服务。
 */
@SpringBootTest
@AutoConfigureMockMvc
class BusApiTest {

    private final Logger log = LoggerFactory.getLogger(getClass());

    @Autowired
    private MockMvc mvc;

    private final ObjectMapper mapper = new ObjectMapper();

    private String openSession(String credential) throws Exception {
        MvcResult result = mvc.perform(post("/bus/sessions")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"credential\":\"" + credential + "\"}"))
                .andExpect(status().isOk())
                .andReturn();
        JsonNode body = mapper.readTree(result.getResponse().getContentAsString());
        String sessionId = body.get("sessionId").asText();
        log.info("[会话开启] sessionId={} clientId={}", sessionId, body.get("clientId").asText());
        return sessionId;
    }

    @Test
    void authenticationFailuresAreDistinguishable() throws Exception {
        mvc.perform(post("/bus/sessions").contentType(MediaType.APPLICATION_JSON).content("{}"))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.code").value("UNAUTHENTICATED"));
        log.info("[拒绝] 未携带凭据 -> 401 UNAUTHENTICATED");

        mvc.perform(post("/bus/sessions").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"credential\":\"forged\"}"))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.code").value("AUTHENTICATION_FAILED"));
        log.info("[拒绝] 伪造凭据 -> 401 AUTHENTICATION_FAILED");

        // 受限身份：越权（403）与未认证（401）、未知主题（404）彼此可区分
        String limited = openSession("local-telemetry-token");
        mvc.perform(post("/bus/sessions/{id}/subscriptions", limited)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"topic\":\"robot.commands\"}"))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.code").value("UNAUTHORIZED_SUBSCRIBE"));
        log.info("[拒绝] 遥测令牌订阅指令主题 -> 403 UNAUTHORIZED_SUBSCRIBE");

        String full = openSession("local-robot-token");
        mvc.perform(post("/bus/sessions/{id}/publications", full)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"topic\":\"robot.unknown\",\"payloadBase64\":\"eA==\"}"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.code").value("UNKNOWN_TOPIC"));
        log.info("[拒绝] 未注册主题 -> 404 UNKNOWN_TOPIC");
    }

    @Test
    void fullChainOverHttp() throws Exception {
        String publisher = openSession("local-robot-token");
        String subscriber = openSession("local-robot-token");

        mvc.perform(get("/bus/topics"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.topics[0]").value("robot.commands"));
        log.info("[主题清单] 已通过服务端入口查看");

        mvc.perform(post("/bus/sessions/{id}/subscriptions", subscriber)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"topic\":\"robot.commands\",\"policy\":\"DROP_OLDEST\"}"))
                .andExpect(status().isOk());

        String payload = Base64.getEncoder().encodeToString("cmd-1".getBytes(StandardCharsets.UTF_8));
        mvc.perform(post("/bus/sessions/{id}/publications", publisher)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"topic\":\"robot.commands\",\"payloadBase64\":\"" + payload + "\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.seq").value(1));

        MvcResult pollResult = mvc.perform(post("/bus/sessions/{id}/poll", subscriber)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"topic\":\"robot.commands\",\"max\":10}"))
                .andExpect(status().isOk())
                .andReturn();
        JsonNode poll = mapper.readTree(pollResult.getResponse().getContentAsString());
        assertEquals(1, poll.get("deliveries").size());
        assertEquals(1L, poll.get("deliveries").get(0).get("seq").asLong());
        assertFalse(poll.get("deliveries").get(0).get("redelivered").asBoolean());
        assertTrue(poll.get("events").toString().contains("DELIVERED"),
                "判定依据：拉取响应携带可区分的投递事件");
        log.info("[拉取] seq=1 首投，事件={}", poll.get("events"));

        mvc.perform(post("/bus/sessions/{id}/acks", subscriber)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"topic\":\"robot.commands\",\"seq\":1}"))
                .andExpect(status().isOk());

        // 断线：发布与拉取被明确挡住（409 SESSION_DISCONNECTED）
        mvc.perform(post("/bus/sessions/{id}/disconnect", subscriber))
                .andExpect(status().isOk());
        mvc.perform(post("/bus/sessions/{id}/poll", subscriber)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"topic\":\"robot.commands\",\"max\":10}"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.code").value("SESSION_DISCONNECTED"));
        log.info("[断线拦截] 断线后拉取 -> 409 SESSION_DISCONNECTED");

        mvc.perform(post("/bus/sessions/{id}/publications", publisher)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"topic\":\"robot.commands\",\"payloadBase64\":\"" + payload + "\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.seq").value(2));

        // 重连：从已确认位点续传，只收到 seq=2
        mvc.perform(post("/bus/sessions/{id}/reconnect", subscriber))
                .andExpect(status().isOk());
        MvcResult resumed = mvc.perform(post("/bus/sessions/{id}/poll", subscriber)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"topic\":\"robot.commands\",\"max\":10}"))
                .andExpect(status().isOk())
                .andReturn();
        JsonNode resumedBody = mapper.readTree(resumed.getResponse().getContentAsString());
        assertEquals(1, resumedBody.get("deliveries").size());
        assertEquals(2L, resumedBody.get("deliveries").get(0).get("seq").asLong(),
                "判定依据：重连后从已确认位点续传，已确认的 seq=1 不重复交付");
        log.info("[重连续传] seq=2 首投");
    }
}
