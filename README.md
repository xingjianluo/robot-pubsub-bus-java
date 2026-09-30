# robot-pubsub-bus-java

面向机器人指令下发与遥测上报的本地发布/订阅消息总线（Spring Boot 工程）。
核心实现位于 `com.github.xingjianluo.robotpubsubbusjava.bus` 包，不依赖任何外部服务。
客户端既可以在进程内直接调用 `MessageBus`，也可以通过 HTTP 入口（`/bus/**`）远程驱动整条链路。

## 投递语义

- **至少一次（at-least-once）**：消息在确认前可能被重复投递，消费者需按主题内序号 `seq` 自行去重。
- **会话内有序**：同一订阅按主题内单调递增序号 `seq` 顺序投递。
- **首次投递与重投可区分**：`Delivery.redelivered` 标志与 `Delivery.attempt`（投递次数，从 1 开始）
  明确区分首次投递与重投。
- **确认机制**：`ack(sessionId, topic, seq)` 为累积确认，确认位点推进到 `seq`（含），
  已确认消息不再投递。投递后 `ack-timeout-millis` 内未确认的消息将被重投。

## 对外 HTTP 入口

启动本地服务（默认 8080 端口）：

```bash
mvn spring-boot:run
```

所有接口位于 `/bus` 下，报文负载以 Base64 编码。会话身份只在**建会话时**由服务端核验：
客户端提交凭据，服务端认下并核验为身份（`SessionAuthenticator`），之后所有操作以会话标识进行，
客户端不能自称是谁就拿到谁的发布/订阅权限。未携带凭据（`UNAUTHENTICATED`）或凭据非法
（`AUTHENTICATION_FAILED`）直接拒绝，与越权、未知主题等失败可区分。

| 方法与路径 | 说明 |
| --- | --- |
| `POST /bus/sessions` | 建会话，请求体 `{"credential":"..."}`，返回 `{sessionId, clientId}` |
| `GET /bus/topics` | 查看已注册主题清单 |
| `POST /bus/sessions/{id}/subscriptions` | 订阅，`{"topic":"...","policy":"DROP_OLDEST"}`（policy 可省略） |
| `POST /bus/sessions/{id}/publications` | 发布，`{"topic":"...","payloadBase64":"..."}`，返回 `{topic, seq}` |
| `POST /bus/sessions/{id}/poll` | 拉取，`{"topic":"...","max":10,"waitMillis":0}`，返回 `{deliveries, events}` |
| `POST /bus/sessions/{id}/acks` | 累积确认，`{"topic":"...","seq":N}` |
| `POST /bus/sessions/{id}/disconnect` | 断线（保留订阅与确认位点） |
| `POST /bus/sessions/{id}/reconnect` | 重连（从确认位点续传，被摘除订阅复活） |
| `POST /bus/sessions/{id}/close` | 关闭会话（终态） |
| `GET /bus/sessions/{id}/events` | 读取并清空该会话的客户端可见事件 |

失败响应统一为 `{"code":"...","message":"..."}`，HTTP 状态码与错误码一一对应
（401 未认证/凭据非法，403 越权，404 未知主题/会话/订阅，409 断线/已关闭/订阅被摘除，400 报文非法/超限）。

## 投递事件反馈

客户端通过拉取响应的 `events` 字段（或 `GET .../events`）获得可区分的事件流，
据此判断"我收到了 / 这段我丢了 / 我这个会话没了"：

| 事件类别 | 含义 |
| --- | --- |
| `DELIVERED` | 正常投递，`fromSeq` 为被投递消息序号 |
| `GAP` | 背压丢弃，`[fromSeq, toSeq]` 为丢失的连续序号区间，`missingSeqs` 列出缺口 |
| `SUBSCRIPTION_EVICTED` | 订阅被摘除；会话保留，重连后从已确认位点续传 |
| `SESSION_CLOSED` | 会话被服务端关闭，不可再使用 |

连续的丢弃会合并为一段缺口，例如 `GAP [2,6]` 表示 seq 2 到 6 全部丢失。

## 重连与断线规则

- `disconnect` 断开会话时保留全部订阅状态与确认位点，并将未确认消息置为待重投。
- **断线真正生效**：断线后该会话的发布、拉取、确认、订阅均被 `SESSION_DISCONNECTED` 明确挡住，
  只有重连回来才能继续，不会出现断线了还能拉到消息的情况。
- `reconnect` 以原会话标识恢复会话，从已确认位点之后继续投递：
  未确认消息以 `redelivered=true` 优先重投，随后按序投递新消息；
  因背压被摘除的订阅随重连复活（确认位点保留）。
- 已确认位点之前（含位点）的消息绝不重复交付；重连未知会话以 `SESSION_NOT_FOUND` 拒绝。
- `closeSession` 为终态：此后任何操作以 `SESSION_CLOSED` 拒绝。

## 背压策略与资源上限

- 每个订阅的积压队列有界（`bus.max-backlog-per-subscription`）。
- **会话级总量上限**：单会话全部订阅积压之和受 `bus.max-backlog-per-session` 约束，
  不随订阅主题数无界增长；多主题同时积压也不能突破总量。
- 超限时的处理**按订阅选择**（订阅时指定 `policy`）：
  - `DROP_OLDEST`（默认）：丢弃最旧消息为新消息腾位，产生 `GAP` 事件；
    连续丢弃达到 `bus.max-drops-before-disconnect` 次后订阅被摘除。
  - `EVICT_SUBSCRIPTION`：一旦超限即把整个订阅摘除，产生 `GAP` + `SUBSCRIPTION_EVICTED` 事件。
- 订阅被摘除后**会话本身保留**，重连后仍能从已确认位点接着收，不会因一次背压把整个会话作废。
- 拉取可带 `waitMillis` 有界等待新消息，实际等待不超过 `bus.max-poll-wait-millis`，
  且只阻塞在该订阅自身——慢会话不会拖住其他会话或其他订阅的发布与拉取。
- 并发保证：同一主题的序号分配与投递入队在同一临界区完成，并发发布/拉取/确认交错时
  序号与确认位点不串位（有并发测试覆盖）。

## 错误分类

所有失败以 `BusException` 携带 `ErrorCode` 显式抛出（HTTP 入口映射为对应状态码），
不静默断开、不静默丢消息：

| 错误码 | 含义 |
| --- | --- |
| `UNAUTHENTICATED` | 建会话未携带身份凭据 |
| `AUTHENTICATION_FAILED` | 身份凭据非法或不被服务端认可 |
| `UNKNOWN_TOPIC` | 主题未在服务端注册 |
| `UNAUTHORIZED_PUBLISH` | 会话身份未被授权向该主题发布 |
| `UNAUTHORIZED_SUBSCRIBE` | 会话身份未被授权订阅该主题 |
| `MALFORMED_MESSAGE` | 空报文或结构非法 |
| `MESSAGE_TOO_LARGE` | 报文超过 `bus.max-payload-bytes` 上限 |
| `SESSION_NOT_FOUND` | 未知会话（如重连携带无效会话标识） |
| `SESSION_DISCONNECTED` | 会话处于断线状态，数据操作被挡住 |
| `SESSION_CLOSED` | 会话已被关闭（终态） |
| `SUBSCRIPTION_NOT_FOUND` | 订阅不存在 |
| `SUBSCRIPTION_EVICTED` | 订阅已因背压策略被摘除 |

授权基于服务端确认的会话身份 `SessionIdentity`（发布/订阅主题集合，`"*"` 表示全部主题）。
本地部署使用 `StaticTokenAuthenticator`：令牌 → 身份的映射由服务端配置（见下），
客户端只持有令牌，身份与权限始终由服务端认下。

## 配置项（application.properties）

| 键 | 默认值 | 含义 |
| --- | --- | --- |
| `bus.max-backlog-per-subscription` | 100 | 单订阅积压上限 |
| `bus.ack-timeout-millis` | 5000 | 确认超时，超时重投 |
| `bus.max-payload-bytes` | 65536 | 单条报文字节上限 |
| `bus.max-drops-before-disconnect` | 3 | DROP_OLDEST 连续丢弃摘除阈值 |
| `bus.max-backlog-per-session` | 1000 | 单会话全部订阅积压之和的总上限 |
| `bus.max-poll-wait-millis` | 1000 | 单次拉取最长等待毫秒数 |
| `bus.topics` | robot.commands,robot.telemetry | 启动时注册的主题 |
| `bus.tokens[n].token` / `.client-id` / `.publish-topics` / `.subscribe-topics` | 见配置文件 | 本地令牌表（仅本地验证用，非真实凭据） |

## 兼容范围

- 进程内 API 保持兼容：`openSession(SessionIdentity)`、`subscribe(sessionId, topic)`、
  `publish`、`poll(sessionId, topic, max)`、`ack`、`disconnect`、`reconnect`、
  `drainDropEvents`、`ackedPosition` 的签名与语义不变；`BusConfig` 旧的四参构造保留
  （会话总上限默认为单订阅上限的 10 倍，拉取等待上限 1 秒）。
- 至少一次、会话内有序、累积确认、首投/重投可区分的语义不变。
- 一处行为收紧：断线会话的发布/拉取/确认/订阅现在会被 `SESSION_DISCONNECTED` 明确拒绝
  （此前只是记录状态）；订阅被摘除后重连可复活续传（此前永久拒绝）。

## 本地验证

不依赖外部服务、不使用真实凭据，直接运行：

```bash
mvn test
```

测试覆盖（日志打印会话标识、消息序号、确认位点与判定依据）：

- `BusApiTest`：HTTP 入口全链路（建会话/鉴权/订阅/发布/拉取/确认/断线拦截/重连续传/主题清单）
  与各类失败的可区分错误码；
- `AuthenticationTest`：凭据由服务端核验，未携带/非法凭据与越权、未知主题可区分；
- `DisconnectEnforcementTest`：断线后发布/拉取被明确挡住，重连恢复；
- `DeliveryEventsTest`：DELIVERED / GAP（缺失序号缺口）/ SUBSCRIPTION_EVICTED / SESSION_CLOSED 可区分；
- `BackpressurePolicyTest`：按订阅选择背压策略，摘除后会话保留并重连续传；
- `SessionCapacityTest`：会话级总量上限跨主题生效，多主题积压不突破总量；
- `ConcurrencyTest`：并发发布/拉取/确认不串位，慢会话有界等待不拖住他人；
- 回归：`ReconnectResumeTest`（断线重连续传）、`RedeliveryTest`（超时重投）、
  `AuthorizationTest`（越权与失败语义）、`SlowConsumerTest`（慢消费者隔离）。
