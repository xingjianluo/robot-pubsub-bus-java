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
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * 对外 HTTP 入口的会话级总上限回归：一个主题把会话总量占满、另一个主题积压为空时，
 * 继续向空主题发布必须返回 200（容量到顶按背压策略正常处理），
 * 不得返回 500；且同一条消息对其他会话的投递不被掐断。
 *
 * <p>用小的资源上限覆盖默认配置，使会话总量在几条消息内即可占满。
 */
@SpringBootTest(properties = {
        "bus.max-backlog-per-subscription=4",
        "bus.max-backlog-per-session=4",
        "bus.max-drops-before-disconnect=100"
})
@AutoConfigureMockMvc
class SharedSessionCapApiTest {

    private final Logger log = LoggerFactory.getLogger(getClass());

    @Autowired
    private MockMvc mvc;

    private final ObjectMapper mapper = new ObjectMapper();

    private String openSession() throws Exception {
        MvcResult result = mvc.perform(post("/bus/sessions")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"credential\":\"local-robot-token\"}"))
                .andExpect(status().isOk())
                .andReturn();
        return mapper.readTree(result.getResponse().getContentAsString())
                .get("sessionId").asText();
    }

    private void subscribe(String sessionId, String topic) throws Exception {
        mvc.perform(post("/bus/sessions/{id}/subscriptions", sessionId)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"topic\":\"" + topic + "\"}"))
                .andExpect(status().isOk());
    }

    private void publishOk(String sessionId, String topic, String text, long expectedSeq) throws Exception {
        String payload = Base64.getEncoder().encodeToString(text.getBytes(StandardCharsets.UTF_8));
        mvc.perform(post("/bus/sessions/{id}/publications", sessionId)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"topic\":\"" + topic + "\",\"payloadBase64\":\"" + payload + "\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.seq").value(expectedSeq));
    }

    @Test
    void publishToEmptySubscriptionWithFullSessionCapReturns200AndKeepsOthers() throws Exception {
        String publisher = openSession();
        String slow = openSession();
        String other = openSession();
        subscribe(slow, "robot.commands");
        subscribe(slow, "robot.telemetry");
        subscribe(other, "robot.telemetry");

        // 主题 robot.commands 独自占满会话总量（4 条）
        for (int i = 1; i <= 4; i++) {
            publishOk(publisher, "robot.commands", "cmd-" + i, i);
        }

        // 向积压为空的 robot.telemetry 发布：必须是 200 而不是 500
        publishOk(publisher, "robot.telemetry", "tm-1", 1);
        publishOk(publisher, "robot.telemetry", "tm-2", 2);
        log.info("[背压] 会话总量已满，向空订阅发布两次均返回 200");

        // 慢会话的拉取响应里应携带缺口事件，完整反映被丢弃的 seq=1,2
        MvcResult slowPoll = mvc.perform(post("/bus/sessions/{id}/poll", slow)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"topic\":\"robot.telemetry\",\"max\":10}"))
                .andExpect(status().isOk())
                .andReturn();
        JsonNode slowBody = mapper.readTree(slowPoll.getResponse().getContentAsString());
        assertEquals(0, slowBody.get("deliveries").size(),
                "判定依据：空订阅无最旧可丢，新到消息被丢弃，不向慢会话投递");
        String events = slowBody.get("events").toString();
        log.info("[缺口事件] 慢会话事件={}", events);
        org.junit.jupiter.api.Assertions.assertTrue(events.contains("GAP"),
                "判定依据：超限丢弃以缺口事件对客户端可见");

        // 其他会话对同一条消息的投递不被掐断
        MvcResult otherPoll = mvc.perform(post("/bus/sessions/{id}/poll", other)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"topic\":\"robot.telemetry\",\"max\":10}"))
                .andExpect(status().isOk())
                .andReturn();
        JsonNode otherBody = mapper.readTree(otherPoll.getResponse().getContentAsString());
        assertEquals(2, otherBody.get("deliveries").size(),
                "判定依据：某一会话超限不影响该消息对其他订阅者的投递");
        assertEquals(1L, otherBody.get("deliveries").get(0).get("seq").asLong());
        assertEquals(2L, otherBody.get("deliveries").get(1).get("seq").asLong());
        log.info("[投递] 其他会话按序收到 seq=1,2");
    }
}
