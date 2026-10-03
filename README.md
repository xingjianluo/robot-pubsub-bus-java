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
- **正在等待中的拉取同样被堵死**：若一次带 `waitMillis` 的拉取当时没有可投递内容、正在等待，
  等待期间会话被断开，这次拉取会被**立即唤醒**（不必等满等待时间）并以
  `SESSION_DISCONNECTED`（HTTP 409）返回，不返回任何消息；断线把未确认消息置为待重投，
  但**重投也不会从这次等待中漏出**。只有重连成功之后的拉取才能继续收
  （未确认消息届时以 `redelivered=true` 重投）。实现上“检查连接→进入等待”与
  “置断线→唤醒等待”在订阅同一监视器内互斥，不存在先判定可等、随后断线却仍取走消息的窗口。
- `reconnect` 以原会话标识恢复会话，从已确认位点之后继续投递：
  未确认消息以 `redelivered=true` 优先重投，随后按序投递新消息；
  因背压被摘除的订阅随重连复活（确认位点保留）。
- 已确认位点之前（含位点）的消息绝不重复交付；重连未知会话以 `SESSION_NOT_FOUND` 拒绝。
- `closeSession` 为终态：同样会唤醒等待中的拉取，此后任何操作以 `SESSION_CLOSED` 拒绝。

## 背压策略与资源上限

- 每个订阅的积压队列有界（`bus.max-backlog-per-subscription`）。
- **会话级总量上限**：单会话全部订阅**排队中**（已入队尚未拉取）积压之和受
  `bus.max-backlog-per-session` 约束，不随订阅主题数无界增长；多主题同时积压也不能突破总量。
  已投递但尚未确认的消息在订阅的未确认跟踪中，不计入排队量，确认或超时重投后相应腾挪。
- **一个主题占满总量、其他主题为空时的行为**：假设会话订阅了 A、B 两个主题，
  A 的积压已占满会话总上限而 B 一条都没有，此时再向 B 发布——发布**正常返回序号**
  （不会服务端报错），B 这次入队按 B 自己选择的背压策略处理：
  - `DROP_OLDEST`：B 自己没有旧消息可丢时，**这条新消息自身记为一条 `GAP` 缺口**
    （它对 B 确实未投递），不入队、会话总占用不变；待 B 自己有积压后，再超限才是丢 B 最旧消息腾位。
  - `EVICT_SUBSCRIPTION`：B 订阅被摘除并产生 `GAP` + `SUBSCRIPTION_EVICTED` 事件。
  - 无论哪种策略，该消息对**其他订阅会话的投递都不受影响**：扇出按订阅会话隔离，
    单个订阅会话的背压失败不会让发布失败，也不会掐断这条消息对其他订阅者的投递。
- **超限后的记账**：每一条被丢弃的消息都恰好对应一个 `GAP` 序号，不凭空多一条、少一条；
  相邻的同主题缺口会合并为连续区间。最终恒等式：
  **某主题发布总数 = 该订阅当前剩余积压（含未确认跟踪）+ 缺口事件序号数**。
- 超限时的处理**按订阅选择**（订阅时指定 `policy`）：
  - `DROP_OLDEST`（默认）：丢弃最旧消息为新消息腾位，产生 `GAP` 事件；
    连续丢弃达到 `bus.max-drops-before-disconnect` 次后订阅被摘除。
  - `EVICT_SUBSCRIPTION`：一旦超限即把整个订阅摘除，产生 `GAP` + `SUBSCRIPTION_EVICTED` 事件。
- 订阅被摘除后**会话本身保留**，其他主题订阅照常可用；重连后被摘除的订阅复活，
  仍从已确认位点接着收，不会因一次背压把整个会话作废。
- 拉取可带 `waitMillis` 有界等待新消息，实际等待不超过 `bus.max-poll-wait-millis`，
  且只阻塞在该订阅自身——慢会话不会拖住其他会话或其他订阅的发布与拉取。
- 并发保证：同一主题的序号分配与投递入队在同一临界区完成，会话容量检查与入队在会话锁内完成；
  并发发布/拉取/确认交错时序号与确认位点不串位，会话总排队量全程不突破上限
  （有并发交错与对账测试覆盖）。

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
- 两处缺陷修复（对外入口边界）：会话总量被其他主题占满、本主题为空时发布不再抛
  服务端内部错误（现按订阅背压策略记缺口，发布正常返回、不连累其他订阅者）；
  断线会立即唤醒并挡住正在 `waitMillis` 等待中的拉取（此前等待结束后仍可能取到消息/重投）。

## 本地验证

不依赖外部服务、不使用真实凭据，直接运行：

```bash
mvn test                       # 全量单测
mvn test -Dtest=SessionCapMultiTopicTest   # 只跑多主题共享会话总上限
mvn test -Dtest=DisruptWaitingPollTest     # 只跑断线打断等待中的拉取
mvn test -Dtest=BusApiEdgeCaseTest         # 只跑 HTTP 入口边界（500/409 回归）
```

测试日志带业务标签（`[发布]`、`[缺口事件]`、`[断连]`、`[等待拉取结果]`、`[并发]`、`[对账]` 等），
可直接看出每类业务覆盖了哪些用例、序号与缺口如何对账、判定依据是什么。

测试覆盖（日志打印会话标识、消息序号、确认位点与判定依据）：

- `SessionCapMultiTopicTest`（**新增**，多主题共享会话总上限的边界）：
  - 一个主题占满会话总量、另一主题为空时继续发布：发布正常返回不报错、空主题消息按策略记缺口、
    其他订阅会话不受连累；
  - 空主题先自丢、腾出容量后再积压时回归“丢最旧”，缺口序号恰好对得上；
  - 空主题触发 `EVICT_SUBSCRIPTION`：只摘订阅不废会话，其他主题仍可用，重连后从确认位点续传；
  - 两个主题并发发布交错：全程采样会话总排队量不突破上限，
    并按“发布总数 = 剩余积压 + 缺口序号数”逐主题对账。
- `DisruptWaitingPollTest`（**新增**，断线对等待中拉取的影响）：
  - 拉取带等待时间、当时无内容，等待中断线：当次拉取被立即唤醒并返回
    `SESSION_DISCONNECTED`（断言唤醒耗时远小于等待时间）、零投递，重连后才收；
  - 断线把未确认消息置为重投时，等待中的拉取也拿不到重投，重连后才以
    `redelivered=true` 收到；
  - 连续多轮“等待→断线→挡住→重连→收消息”行为一致。
- `BusApiEdgeCaseTest`（**新增**，HTTP 入口边界回归）：小容量配置下复现
  “空主题发布曾返回 500”（现 200、缺口事件正确、其他订阅者收齐）与
  “等待中断线曾无响应”（现立即 409 `SESSION_DISCONNECTED`，重连后 200）。
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

### 手工复现要点

- **会话总上限多主题**：把 `bus.max-backlog-per-subscription` 与
  `bus.max-backlog-per-session` 都设为 5，同一会话订阅 `robot.commands` 与
  `robot.telemetry`；不消费连发 5 条指令占满总量，再发遥测：修复前 HTTP 500
  （服务端 NPE，序号被跳过且后续订阅者被连累），修复后返回 200 与序号，
  `GET /bus/sessions/{id}/events` 可见恰好覆盖该序号的 `GAP` 事件。
- **断线打断等待拉取**：调大 `bus.max-poll-wait-millis`（如 30000），对无消息的主题发起
  `poll` 且 `waitMillis=30000`，请求挂起后调用 `disconnect`：修复前该请求会等到超时，
  甚至在断线期间返回消息；修复后立即返回 409 `SESSION_DISCONNECTED`，`reconnect` 后恢复。
