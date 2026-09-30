# robot-pubsub-bus-java

面向机器人指令下发与遥测上报的本地发布/订阅消息总线（Spring Boot 工程）。
核心实现位于 `com.github.xingjianluo.robotpubsubbusjava.bus` 包，不依赖任何外部服务。

它既可以作为进程内库直接调用（`MessageBus`），也可以在本地启动一个 HTTP 服务
（`com.github.xingjianluo.robotpubsubbusjava.web`），让真正的网络客户端驱动
建会话、订阅、发布、拉取、确认、断线重连与主题清单查询。

## 一、网络入口（客户端如何真正驱动）

所有接口在 `/api` 下，请求/响应均为 JSON，消息负载统一用 Base64 传输。

| 方法与路径 | 含义 |
| --- | --- |
| `POST /api/sessions` | 携带 `X-Client-Token` 建会话，返回服务端生成的 `sessionId` 与认下的 `clientId` |
| `GET  /api/topics` | 查看已注册主题清单 |
| `POST /api/sessions/{id}/subscriptions` | 订阅主题，请求体可指定背压策略 `strategy` |
| `POST /api/sessions/{id}/messages` | 发布消息 `{topic, payloadBase64}`，返回主题序号 `seq` |
| `GET  /api/sessions/{id}/subscriptions/{topic}/messages?max=&wait=` | 拉取投递；`wait=true` 时有上限地长轮询 |
| `POST /api/sessions/{id}/acks` | 累积确认 `{topic, seq}` |
| `GET  /api/sessions/{id}/events` | 读取并清空事件（缺口 / 订阅摘除 / 会话关闭） |
| `POST /api/sessions/{id}/disconnect` | 断线（保留订阅与位点） |
| `POST /api/sessions/{id}/reconnect` | 重连，从确认位点续传并重新挂上被摘除的订阅 |
| `DELETE /api/sessions/{id}` | 永久关闭会话（成功返回 204） |

除建会话与主题清单外，每个接口都必须带回本人的 `X-Client-Token`，服务端核验
令牌与 `sessionId` 的归属。

### 鉴权与身份核验

- 会话身份**由服务端认下**：客户端只能凭服务端登记的令牌建会话，身份（clientId、
  可发布/可订阅主题集合）由服务端登记材料推导，客户端不能自称是谁。
- 未携带令牌 → `401 UNAUTHENTICATED`；令牌不合法或与会话归属不匹配
  → `403 INVALID_CREDENTIALS`。这两者与越权、未知主题明确区分：

| 场景 | HTTP 状态 | 错误码 |
| --- | --- | --- |
| 没带令牌 | 401 | `UNAUTHENTICATED` |
| 令牌不合法 / 令牌与他人会话不匹配 | 403 | `INVALID_CREDENTIALS` |
| 越权发布 / 越权订阅 | 403 | `UNAUTHORIZED_PUBLISH` / `UNAUTHORIZED_SUBSCRIBE` |
| 未知主题 / 未知订阅 / 未知会话 | 404 | `UNKNOWN_TOPIC` / `SUBSCRIPTION_NOT_FOUND` / `SESSION_NOT_FOUND` |
| 断线中操作 | 409 | `SESSION_DISCONNECTED` |
| 订阅被摘除后拉取 | 409 | `SUBSCRIPTION_EVICTED` |
| 会话已关闭 | 410（关闭动作本身 204） | `SESSION_CLOSED` |
| 空报文 / 非法 JSON / 非法 Base64 | 400 | `MALFORMED_MESSAGE` |
| 报文超限 | 413 | `MESSAGE_TOO_LARGE` |

登记客户端在 `application.properties` 的 `bus.clients[...]` 配置
（token / client-id / publish-topics / subscribe-topics，`"*"` 表示全部主题）。
内置演示客户端 `commander-token` 与 `observer-token`，本地无需真实凭据。

### 断线是真的生效

- 断线后该会话的**发布、拉取、订阅都被明确拒绝**（`SESSION_DISCONNECTED`），
  不会出现“断线了还能拉到消息”。
- 断线期间其他发布者仍可为该订阅缓冲消息；重连后未确认消息以重投优先返回，
  随后按序投递断线期缓冲的新消息。
- `DELETE` 关闭会话是**永久**作废：产生 `SESSION_CLOSED` 事件，重连以
  `SESSION_CLOSED` 拒绝；关闭事件仍可通过 events 接口读到。

## 二、投递语义（保持不变）

- **至少一次（at-least-once）**：消息在确认前可能重复投递，消费者按主题内 `seq` 去重。
- **会话内有序**：同一订阅按主题内单调递增序号 `seq` 顺序投递。并发多生产者下，
  总线在订阅内按 `seq` 维护“连续可投递前沿”，更小号未到达时更大的号先按住，绝不乱序。
- **首次投递与重投可区分**：`Delivery.redelivered` 与 `Delivery.attempt`（从 1 开始）。
- **累积确认**：`ack(sessionId, topic, seq)` 把确认位点推进到 `seq`（含），
  已确认消息不再投递；`ack-timeout-millis` 内未确认的消息超时重投。

## 三、投递事件与背压策略

客户端通过 events 接口（进程内为 `drainEvents`）读取三类**彼此可区分**的事件，
据此判断“我收到了 / 这段我丢了 / 我这个订阅或会话没了”：

| 事件类型 | 含义 |
| --- | --- |
| `BACKLOG_GAP` | 因背压丢了一段序号，`gaps` 给出连续缺失区间 `[{fromSeq,toSeq}]`，形成可识别缺口 |
| `SUBSCRIPTION_EVICTED` | 订阅被摘除（携带被作废序号的 gaps）；会话本身保留 |
| `SESSION_CLOSED` | 会话被永久关闭 |

正常投递本身不是事件，通过 poll 返回的 `Delivery`（seq / redelivered / attempt）判定。
摘除发生时会先发 `BACKLOG_GAP`（丢了哪些序号）再发 `SUBSCRIPTION_EVICTED`，两类不混淆。

积压达到上限时的处理策略**按订阅**在订阅时通过 `strategy` 选择：

- `DROP_OLDEST`（默认）：丢弃最旧消息腾出纳新空间，每次丢弃产生 `BACKLOG_GAP`；
  连续丢弃达到 `bus.max-drops-before-disconnect` 次后摘除该订阅。
- `EVICT_SUBSCRIPTION`：积压一满就直接摘除整个订阅，作废积压与后续到达序号并上报缺口。

**订阅被摘除 ≠ 会话作废**：会话保留，其他订阅继续可用；重连后被摘除的订阅重新挂上，
从已确认位点继续收，摘除期间缺失的序号以一个合并缺口事件补报。

> 旧的进程内事件 API `drainDropEvents`（`DropEvent` / `DropReason`）仍然保留，
> 兼容既有用法与测试。

## 四、资源上限与隔离

- 单订阅积压上限：`bus.max-backlog-per-subscription`。
- **单会话总缓冲上限**：`bus.max-backlog-per-session`，统计该会话所有订阅的
  “待投递 + 已投递未确认”之和。多主题同时积压也不会突破该总量；总量吃紧时
  丢弃会话内最旧积压并向对应订阅上报缺口。总量不随订阅主题数增多而无界增长。
- **慢会话隔离**：每个会话一把 gate 锁与一个条件变量。慢会话只在自己的条件变量上
  做有上限的等待（`bus.poll-wait-millis`，HTTP `wait=true`），不会拖住其他会话、
  其他订阅的发布与拉取；发布者扇出时只短暂获取各订阅者会话的锁。
- 并发发布 / 拉取 / 确认在同一会话内串行化，序号与累积确认位点不串位。

## 五、配置项（application.properties）

| 键 | 默认值 | 含义 |
| --- | --- | --- |
| `bus.topics` | robot.commands,robot.telemetry | 启动注册主题 |
| `bus.max-backlog-per-subscription` | 100 | 单订阅积压上限 |
| `bus.max-backlog-per-session` | 400 | 单会话跨订阅总缓冲上限 |
| `bus.ack-timeout-millis` | 5000 | 确认超时，超时重投 |
| `bus.max-payload-bytes` | 65536 | 单条负载字节上限 |
| `bus.max-drops-before-disconnect` | 3 | DROP_OLDEST 连续丢弃摘除阈值 |
| `bus.poll-wait-millis` | 5000 | 长轮询最多等待毫秒（慢消费者只阻塞自己） |
| `bus.clients[...]` | commander / observer | 服务端登记的客户端令牌与授权主题 |

## 六、本地验证

不依赖外部服务、不用真实凭据，直接运行：

```bash
mvn test
```

测试（17 个，均可反复本地运行）：

- `HttpApiIntegrationTest`：真实本地端口的端到端——建会话鉴权（缺令牌/坏令牌/越权/
  未知主题/令牌归属可区分）、订阅发布拉取确认全链路、断线 409 拦截与重连续传、
  事件分类、订阅摘除后会话保留与重连续传、关闭会话；
- `BackpressureStrategyTest`：EVICT_SUBSCRIPTION 与 DROP_OLDEST 两类策略、
  缺口事件与摘除事件区分、摘除后重挂从位点续收；
- `SessionCapacityTest`：多主题会话总缓冲不突破上限、慢会话长轮询不阻塞其他会话、
  并发发布/拉取/确认序号与位点不串位；
- `DisconnectStateTest`：断线拦截发布/拉取/订阅、断线期缓冲与重连续传、
  关闭会话事件与不可重连、未知/关闭会话错误区分；
- `ReconnectResumeTest` / `RedeliveryTest` / `AuthorizationTest` / `SlowConsumerTest`：
  既有断线重连、超时重投、越权与慢消费者用例，确认不回归。

也可实际启动后用 curl 驱动：

```bash
mvn spring-boot:run
# 建会话（演示令牌）
curl -X POST -H "X-Client-Token: commander-token" localhost:8080/api/sessions
```

## 七、兼容范围

- 进程内用法 `openSession(SessionIdentity)` / `subscribe(id,topic)` / `poll` / `ack` /
  `disconnect` / `reconnect` / `drainDropEvents` 签名与语义保持兼容。
- 既有“至少一次、会话内有序、累积确认、首投/重投可区分、断线从位点续传”语义不变；
  新增能力（令牌鉴权、事件、背压策略、会话总量、关闭会话）均为增量。
