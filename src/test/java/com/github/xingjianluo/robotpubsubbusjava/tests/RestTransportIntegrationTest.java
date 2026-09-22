package com.github.xingjianluo.robotpubsubbusjava.tests;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import org.springframework.http.MediaType;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * 场景四（传输层）：HTTP 端到端的身份认证、主题级授权与统一失败语义。
 * 不依赖外部服务，使用 application.properties 内置身份与主题。
 */
@SpringBootTest
@AutoConfigureMockMvc
class RestTransportIntegrationTest {

    @Autowired
    private MockMvc mvc;

    @Test
    void unauthenticatedRequestIsRejectedWithVisibleErrorCode() throws Exception {
        mvc.perform(post("/bus/publish").param("topic", "robot.cmd").content("x").contentType(MediaType.APPLICATION_OCTET_STREAM))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.errorCode").value("UNAUTHENTICATED"))
                .andExpect(jsonPath("$.message").exists());
    }

    @Test
    void forbiddenPublishIsDistinguishableFromUnknownTopic() throws Exception {
        // observer 只能访问 robot.telemetry，向 robot.cmd 发布 -> 403 PUBLISH_FORBIDDEN
        mvc.perform(post("/bus/publish")
                        .header("X-Bus-Token", "observer")
                        .param("topic", "robot.cmd").content("hi").contentType(MediaType.APPLICATION_OCTET_STREAM))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.errorCode").value("PUBLISH_FORBIDDEN"));

        // commander 向未注册主题发布 -> 404 UNKNOWN_TOPIC（与越权可区分）
        mvc.perform(post("/bus/publish")
                        .header("X-Bus-Token", "cmd-controller")
                        .param("topic", "robot.nope").content("hi").contentType(MediaType.APPLICATION_OCTET_STREAM))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.errorCode").value("UNKNOWN_TOPIC"));
    }

    @Test
    void oversizedPayloadReturnsPayloadTooLarge() throws Exception {
        byte[] big = new byte[5000]; // robot.cmd 上限 4096
        mvc.perform(post("/bus/publish")
                        .header("X-Bus-Token", "cmd-controller")
                        .param("topic", "robot.cmd").content(big).contentType(MediaType.APPLICATION_OCTET_STREAM))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errorCode").value("PAYLOAD_TOO_LARGE"));
    }

    @Test
    void fullLifecycleConnectSubscribePublishAckAndTopics() throws Exception {
        // 主题清单可见
        mvc.perform(get("/bus/topics"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].name").exists());

        // 机器人建立会话并订阅指令主题
        String connectBody = mvc.perform(post("/bus/sessions").header("X-Bus-Token", "robot-01"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.sessionId").exists())
                .andExpect(jsonPath("$.resumeToken").exists())
                .andReturn().getResponse().getContentAsString();
        String sessionId = com.jayway.jsonpath.JsonPath.read(connectBody, "$.sessionId");

        mvc.perform(post("/bus/subscriptions")
                        .header("X-Bus-Token", "robot-01")
                        .param("sessionId", sessionId)
                        .param("topic", "robot.cmd"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.subscriptionId").exists());

        // 指挥官发布一条指令，机器人确认
        mvc.perform(post("/bus/publish")
                        .header("X-Bus-Token", "cmd-controller")
                        .param("topic", "robot.cmd").content("move-forward").contentType(MediaType.APPLICATION_OCTET_STREAM))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.seq").value(1));

        mvc.perform(post("/bus/ack")
                        .header("X-Bus-Token", "robot-01")
                        .param("sessionId", sessionId)
                        .param("topic", "robot.cmd")
                        .param("seq", "1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.committedSeq").value(1));

        // 越权使用他人会话 -> 403
        mvc.perform(post("/bus/ack")
                        .header("X-Bus-Token", "observer")
                        .param("sessionId", sessionId)
                        .param("topic", "robot.cmd")
                        .param("seq", "1"))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.errorCode").value("RESUME_FORBIDDEN"));
    }
}
