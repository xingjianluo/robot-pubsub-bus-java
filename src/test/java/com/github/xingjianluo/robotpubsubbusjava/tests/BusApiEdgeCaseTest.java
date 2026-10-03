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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * 对外 HTTP 入口的边界回归：
 * <ul>
 *   <li>多主题共享会话总上限：一个主题占满总量后往空主题发布，曾经返回 500，
 *       现在必须 200 正常返回序号，缺口事件完整，其他订阅者不受连累；</li>
 *   <li>断线对正在等待的拉取立即生效：等待期间断线，当次拉取 409 返回，
 *       不返回消息，重连后才能继续收。</li>
 * </ul>
 */
@SpringBootTest(properties = {
        "bus.max-backlog-per-subscription=5",
        "bus.max-backlog-per-session=5",
        "bus.max-drops-before-disconnect=100",
        "bus.max-poll-wait-millis=30000"
})
@AutoConfigureMockMvc
class BusApiEdgeCaseTest {

    private final Logger log = LoggerFactory.getLogger(getClass());

    @Autowired
    private MockMvc mvc;

    private final ObjectMapper mapper = new ObjectMapper();

    private String openSession() throws Exception {
        MvcResult result = mvc.perform(post("/bus/sessions")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"credential\":\"local-robot-token\"}"))
                .andExpect(status().isOk()).andReturn();
        return mapper.readTree(result.getResponse().getContentAsString()).get("sessionId").asText();
    }

    private static String b64(String text) {
        return Base64.getEncoder().encodeToString(text.getBytes(StandardCharsets.UTF_8));
    }

    private void subscribe(String sessionId, String topic, String policy) throws Exception {
        String content = "{\"topic\":\"" + topic + "\""
                + (policy == null ? "" : ",\"policy\":\"" + policy + "\"") + "}";
        mvc.perform(post("/bus/sessions/{id}/subscriptions", sessionId)
                        .contentType(MediaType.APPLICATION_JSON).content(content))
                .andExpect(status().isOk());
    }

    private long publish(String sessionId, String topic, String text) throws Exception {
        MvcResult result = mvc.perform(post("/bus/sessions/{id}/publications", sessionId)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"topic\":\"" + topic + "\",\"payloadBase64\":\"" + b64(text) + "\"}"))
                .andExpect(status().isOk())
                .andReturn();
        return mapper.readTree(result.getResponse().getContentAsString()).get("seq").asLong();
    }

    @Test
    void publishingToEmptyTopicUnderSessionCapReturnsOkNotInternalError() throws Exception {
        String publisher = openSession();
        String slow = openSession();
        String peer = openSession();
        subscribe(slow, "robot.commands", null);
        subscribe(slow, "robot.telemetry", "DROP_OLDEST");
        subscribe(peer, "robot.telemetry", null);

        // 指令主题积压 5 条占满会话总上限
        for (int i = 1; i <= 5; i++) {
            assertEquals(i, publish(publisher, "robot.commands", "cmd-" + i));
        }
        // 往一条都没积压的遥测主题继续发布：曾触发 NPE -> 500
        long seq1 = publish(publisher, "robot.telemetry", "tm-1");
        long seq2 = publish(publisher, "robot.telemetry", "tm-2");
        log.info("[HTTP发布] 会话总量被占满时空主题发布正常返回序号={},{}", seq1, seq2);
        assertEquals(1L, seq1);
        assertEquals(2L, seq2);

        // 缺口事件恰好覆盖 1,2
        MvcResult eventsResult = mvc.perform(get("/bus/sessions/{id}/events", slow))
                .andExpect(status().isOk()).andReturn();
        JsonNode events = mapper.readTree(eventsResult.getResponse().getContentAsString()).get("events");
        StringBuilder gapSeqs = new StringBuilder();
        for (JsonNode event : events) {
            if ("GAP".equals(event.get("kind").asText())
                    && "robot.telemetry".equals(event.get("topic").asText())) {
                long from = event.get("fromSeq").asLong();
                long to = event.get("toSeq").asLong();
                for (long s = from; s <= to; s++) {
                    gapSeqs.append(s);
                }
            }
        }
        log.info("[HTTP缺口事件] 空主题缺口序号={} 判定依据=不多不少", gapSeqs);
        assertEquals("12", gapSeqs.toString(), "判定依据：缺口事件完整反映被丢弃序号");

        // 其他订阅者正常收齐，不被连累
        MvcResult peerPoll = mvc.perform(post("/bus/sessions/{id}/poll", peer)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"topic\":\"robot.telemetry\",\"max\":10}"))
                .andExpect(status().isOk()).andReturn();
        JsonNode peerDeliveries = mapper.readTree(
                peerPoll.getResponse().getContentAsString()).get("deliveries");
        assertEquals(2, peerDeliveries.size(), "判定依据：其他订阅者不受慢会话容量超限连累");
        log.info("[HTTP隔离] peer 订阅者收到遥测消息数={}", peerDeliveries.size());
    }

    @Test
    void waitingPollGetsConflictImmediatelyWhenSessionDisconnects() throws Exception {
        String subscriber = openSession();
        subscribe(subscriber, "robot.commands", null);

        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            // 请求等待 30 秒；服务端配置上限同为 30 秒，当时无消息可投递
            Future<MvcResult> waiting = pool.submit(() -> mvc.perform(
                            post("/bus/sessions/{id}/poll", subscriber)
                                    .contentType(MediaType.APPLICATION_JSON)
                                    .content("{\"topic\":\"robot.commands\",\"max\":10,\"waitMillis\":30000}"))
                    .andReturn());
            Thread.sleep(500); // 确保请求已在服务端等待

            long start = System.nanoTime();
            mvc.perform(post("/bus/sessions/{id}/disconnect", subscriber))
                    .andExpect(status().isOk());
            log.info("[HTTP断线] 等待中的拉取应立即收到 409");

            MvcResult result = waiting.get(10, TimeUnit.SECONDS);
            long elapsedMillis = (System.nanoTime() - start) / 1_000_000;
            // 注意：MockMvc 中异常处理后的状态直接体现在 MvcResult 上
            assertEquals(409, result.getResponse().getStatus(),
                    "判定依据：等待中断线，当次拉取必须返回 409 而非等到超时");
            String body = result.getResponse().getContentAsString();
            assertTrue(body.contains("SESSION_DISCONNECTED"),
                    "判定依据：409 响应携带可区分错误码");
            log.info("[HTTP等待拦截] 状态=409 耗时={}ms body={}", elapsedMillis, body);
            assertTrue(elapsedMillis < 5_000, "判定依据：无需等满等待时间即返回");

            // 断线期间任何拉取（包括有内容的非等待拉取）继续 409
            mvc.perform(post("/bus/sessions/{id}/poll", subscriber)
                            .contentType(MediaType.APPLICATION_JSON)
                            .content("{\"topic\":\"robot.commands\",\"max\":10}"))
                    .andExpect(status().isConflict())
                    .andExpect(jsonPath("$.code").value("SESSION_DISCONNECTED"));

            // 重连后恢复
            mvc.perform(post("/bus/sessions/{id}/reconnect", subscriber))
                    .andExpect(status().isOk());
            mvc.perform(post("/bus/sessions/{id}/poll", subscriber)
                            .contentType(MediaType.APPLICATION_JSON)
                            .content("{\"topic\":\"robot.commands\",\"max\":10}"))
                    .andExpect(status().isOk());
            log.info("[HTTP重连] 拉取恢复 200");
        } finally {
            pool.shutdownNow();
        }
    }
}
