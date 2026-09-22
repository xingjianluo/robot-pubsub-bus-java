# 机器人指令下发 / 遥测上报 本地发布订阅消息总线

面向机器人场景的**进程内本地发布/订阅消息总线**（Spring Boot 3.4 / Java 21，无外部中间件依赖）。
目标是让多路会话在**断连、重连、越权、慢消费**等情况下，仍能得到**可解释、彼此可区分且一致**的投递结果。

- 指令（`robot.cmd`）：控制端发布 → 机器人订阅。
- 遥测（`robot.telemetry`）：机器人发布 → 控制端订阅。

---

## 1. 投递语义（Delivery Semantics）

### 1.1 至少一次 + 主题内有序 + 单在途窗口

| 语义 | 保证 |
| --- | --- |
| 投递次数 | **至少一次**（at-least-once）。正常一次；未确认时可能重投，由客户端按序号幂等去重。 |
| 顺序 | **会话内主题严格有序**。每条订阅维护一个“单在途窗口”：同一时刻只有**最旧未确认序号**在投递，确认/丢弃后窗口才向后滑动。因此重投永远不会越过尚未投递的消息。 |
| 序号 | `seq` 为**主题内全局单调递增**（从 1 开始），发布即分配，与订阅者无关。 |
| 首次/重投 | 每次投递事件带 `deliveryAttempt`：`FIRST`（首次）/`RETRY`（重投），并带自增的 `attemptCount`，客户端可据此区分与打点。 |

### 1.2 三类内部状态（每个订阅独立维护）

- `committed`：累计确认位点（cumulative ack）。所有 `seq <= committed` 视为已确认，**永不重复交付**。
- `inflight`：在途消息（最多 1 条）。投递时启动 ACK 计时。
- `pending`：已发布但尚未进入在途窗口的待发消息，**有界**（见第 3 节）。

`积压(backlog) = inflight 数量 + pending 数量`。

### 1.3 确认（ACK）与超时重投

- 客户端调用 `ack(topic, seq)` 表示 **`<= seq` 全部已处理**，位点直接推进到 `seq`，相关在途/待发/重试状态被清理。
- 在途消息在 `robot.bus.ack-timeout-millis`（默认 2000ms）内未被确认，后台重投扫描线程会把它退回 `pending` 队首并标记 `RETRY`，随后重新投递。
- 已确认的消息**不会**因重投而再次交付（位点已越过）。
- 允许“跳号确认”：确认 `seq=N` 即代表 1..N 全部确认。

---

## 2. 断线重连与去重（Resume & Dedup）

### 2.1 续传令牌

- 建立会话（`POST /bus/sessions`）后返回不透明的 `resumeToken`（Base64URL 编码 `sessionId|principalName`）。
- 重连时携带令牌，服务端校验：
  1. 令牌可解码（畸形 → `RESUME_FORBIDDEN`）；
  2. 令牌声明的身份与当前连接身份一致（冒名接管 → `RESUME_FORBIDDEN`）；
  3. 引用的会话存在；
  4. 该会话当前无其他活动连接（重复接管 → `SESSION_CONFLICT`）。

### 2.2 重连规则

- 主动 `disconnect()` 或网络断开**不会删除会话与位点**。
- 重连成功后：旧连接的在途消息全部退回 `pending`，订阅从 `committed + 1` 续传：
  - 已确认消息（`<= committed`）**绝不重复**；
  - 断线前已投递但未确认的消息，重连后标记为 `RETRY`；
  - 断线期间产生、从未投递过的消息，标记为 `FIRST`。
- 旧连接句柄带“连接代际（generation）”，断线后其任何操作都被拒绝（`SESSION_CLOSED`），防止僵尸连接重复 ACK/消费。
- 主题维护有界的**保留窗口**（`retention-size`）。重连/重新订阅时，若期望位点已被保留窗口覆盖（历史已不可得），会下发**显式 `Gap` 事件**告知缺口区间，而不是静默跳过。

### 2.3 慢消费被摘除后的续传

`DISCONNECT` 策略摘除的是**订阅**而非会话（见 3.3）。恢复方式二选一：

1. 在同一活连接上对该主题再次 `subscribe`；
2. 断开后用 `resumeToken` 重连。

两者都会从已确认位点 + 保留窗口续传；摘除前投递过未确认的消息为 `RETRY`，其余为 `FIRST`。

---

## 3. 背压与资源上限（Backpressure & Resource Bounds）

所有缓冲都是**有界**的，任何一个慢消费者都**不会**：无界占用内存、阻塞发布线程、拖垮其他订阅者。

### 3.1 上限

- 每个主题配置 `max-inflight`：单个订阅者的积压上限（`inflight + pending <= max-inflight`）。
- 每个会话有有界事件队列（容量为各主题 `max-inflight` 之和 + 余量），发布线程向其投递时仅**有界等待 50ms**，不会无限挂起。
- 主题保留窗口 `retention-size` 有界；超限自动淘汰最旧历史。
- 发布报文受 `max-payload-bytes` 约束。

### 3.2 三种确定策略（按主题配置，结果彼此可区分）

| 策略 | 积压满时的行为 | 客户端如何感知 |
| --- | --- | --- |
| `REJECT` | **整笔发布被拒绝**：在扇出给任何订阅者之前统一裁决，只要有一个订阅者无空位即抛错，消息不进入任何队列、不分配语义后果（仍分配 seq 进入主题保留窗口）。订阅者 ACK 释放积压后发布自动恢复。 | 发布者收到错误 `SUBSCRIBER_BACKPRESSURE_REJECTED`（HTTP 503）。 |
| `DROP_OLDEST` | 丢弃该慢订阅者**全局最旧的未确认消息**（在途优先，其次待发队首），确认位点顺移到被丢弃序号，为新消息腾位；新消息继续按序投递。发布永不失败。 | 订阅者收到显式 `Dropped(fromSeq,toSeq,reason)` 事件；被丢弃消息不再重投。 |
| `DISCONNECT` | **摘除（evict）该慢订阅**：仅作用于这一条订阅，订阅级隔离——同会话的其他订阅与发布能力不受影响。摘除后该订阅休眠（不再扇出，零增长），位点保留。 | 订阅者收到显式 `SessionClosed(subscriptionId, reasonCode=SLOW_CONSUMER_DISCONNECTED, message)`；对被摘除订阅 ACK 会得到 `SLOW_CONSUMER_DISCONNECTED` 错误，提示重新订阅续传。 |

> “拒绝 / 丢弃 / 断开”三类结果分别由
> `SUBSCRIBER_BACKPRESSURE_REJECTED`、`Dropped` 事件、`SLOW_CONSUMER_DISCONNECTED` 表达，**原因明确可区分**。

### 3.3 慢消费者为何不影响别人

- 发布路径对每个订阅者独立处理；某订阅者事件队列满时只在其自身循环里有界等待，随后摘除该订阅，发布线程随即释放。
- 一个订阅被摘除/积压，不影响同主题的其他订阅者（健康订阅者照常收消息），也不影响其他主题。

---

## 4. 授权与统一失败语义（Auth & Error Classification）

### 4.1 身份与主题级授权

- 所有请求经 `X-Bus-Token` 头携带令牌，服务端用 `AuthorizationService` 确认会话身份（`Principal`）。
- 每个身份配置可发布 / 可订阅主题集合，支持：
  - 精确匹配 `robot.cmd`
  - 单段通配（以 `.` 为分隔的前缀匹配 `prefix.*`）
  - 全通配 `*`
- **发布与订阅分别授权**；越权操作被拒绝且错误码可区分。会话归属身份也会校验，不能操作/接管他人会话。

### 4.2 错误码一览（`BusErrorCode`，均对客户端可见，不静默断开/丢消息）

| 错误码 | 含义 | 典型 HTTP |
| --- | --- | --- |
| `MALFORMED_MESSAGE` | 报文畸形（负载为 null、ACK 序号为负、起始位置非法等） | 400 |
| `PAYLOAD_TOO_LARGE` | 报文超出主题 `max-payload-bytes` | 400 |
| `UNKNOWN_TOPIC` | 主题未注册（发布/订阅均可能） | 404 |
| `UNAUTHENTICATED` | 缺少/无效身份令牌 | 403 |
| `PUBLISH_FORBIDDEN` | 身份无权向该主题发布 | 403 |
| `SUBSCRIBE_FORBIDDEN` | 身份无权订阅该主题 | 403 |
| `RESUME_FORBIDDEN` | 续传令牌畸形/冒名/会话归属不符 | 403 |
| `SESSION_CONFLICT` | 会话仍有活动连接，拒绝重复接管 | 403 |
| `SUBSCRIBER_BACKPRESSURE_REJECTED` | 积压满，按 REJECT 拒绝本次发布 | 503 |
| `SLOW_CONSUMER_DISCONNECTED` | 慢订阅被摘除（事件）/ 对其 ACK（错误） | 503 |
| `SESSION_CLOSED` | 会话关闭/旧连接代际失效/已断开需重连 | 404 |
| `SUBSCRIPTION_NOT_FOUND` | 会话未订阅该主题却 ACK | 404 |
| `BUS_CLOSED` | 总线已关闭 | 503 |
| `INTERNAL_ERROR` | 总线内部错误 | 500 |

HTTP 错误响应体形如：

```json
{
  "errorCode": "PUBLISH_FORBIDDEN",
  "description": "身份无权向该主题发布",
  "message": "身份 'observer' 无权向主题 'robot.cmd' 发布"
}
```

会话事件流（`SessionEvent`）是一个密封类型，四种子类型彼此可区分：

- `Delivery`：消息投递（含 `FIRST`/`RETRY` 与 `attemptCount`）
- `Dropped`：DROP_OLDEST 丢弃通知（含序号区间与原因）
- `Gap`：保留窗口缺口通知（含期望序号与最旧可用序号）
- `SessionClosed`：订阅/会话被关闭（含 `reasonCode` 与说明）

---

## 5. HTTP 接口（最小传输层）

基址 `/bus`，除 `/topics` 外均需 `X-Bus-Token` 头。

| 方法 路径 | 作用 |
| --- | --- |
| `POST /bus/sessions?resumeToken=` | 建立会话（可带续传令牌重连），返回 `sessionId/principal/resumeToken` |
| `POST /bus/subscriptions?sessionId&topic&position=EARLIEST|LATEST` | 订阅（对被摘除订阅再次调用即续传） |
| `POST /bus/publish?topic=` （body=字节） | 发布消息，返回 `topic/seq` |
| `POST /bus/ack?sessionId&topic&seq` | 累计确认，返回 `committedSeq` |
| `POST /bus/sessions/disconnect?sessionId` | 主动断开（保留会话与位点） |
| `GET  /bus/topics` | 主题清单与配置 |

> 有序投递、ACK 去重、超时重投、背压等核心语义全部由进程内总线保证；HTTP 层负责身份认证、主题级授权与统一错误响应。

### curl 示例

```bash
# 机器人建立会话并订阅指令
curl -s -XPOST -H 'X-Bus-Token: robot-01' localhost:8080/bus/sessions
curl -s -XPOST -H 'X-Bus-Token: robot-01' \
  'localhost:8080/bus/subscriptions?sessionId=<sid>&topic=robot.cmd'

# 控制端下发指令
curl -s -XPOST -H 'X-Bus-Token: cmd-controller' \
  --data-binary 'move-forward' 'localhost:8080/bus/publish?topic=robot.cmd'

# 机器人确认序号 1
curl -s -XPOST -H 'X-Bus-Token: robot-01' \
  'localhost:8080/bus/ack?sessionId=<sid>&topic=robot.cmd&seq=1'
```

---

## 6. 配置（application.properties）

```properties
robot.bus.ack-timeout-millis=2000          # ACK 超时
robot.bus.reaper-interval-millis=100       # 重投扫描周期

# 身份（键含点号/连字符时用 [键] 包裹）
robot.bus.principals[cmd-controller].name=commander
robot.bus.principals[cmd-controller].publish[0]=robot.cmd
robot.bus.principals[cmd-controller].subscribe[0]=robot.telemetry

# 主题
robot.bus.topics[robot.cmd].policy=REJECT            # REJECT / DROP_OLDEST / DISCONNECT
robot.bus.topics[robot.cmd].max-inflight=4
robot.bus.topics[robot.cmd].retention-size=64
robot.bus.topics[robot.cmd].max-payload-bytes=4096
```

> **重要**：Spring 松散绑定下，含点号的 Map 键（`robot.cmd`、`cmd-controller`）必须写成 `robot.bus.topics[robot.cmd]` 这种方括号形式，否则会被解析为嵌套层级导致配置不生效。

---

## 7. 本地验证方法（不依赖任何外部服务）

前置：JDK 21、Maven 3.9+（依赖已在本地仓库时可加 `-o` 离线）。

```bash
# 运行全部测试（纯内存，可反复执行）
mvn test

# 离线运行
mvn -o test

# 只跑四类核心场景
mvn -o test -Dtest=ReconnectRedeliveryTest   # 断线重连 + 重复投递/去重
mvn -o test -Dtest=AuthorizationTest         # 越权访问 + 统一失败语义
mvn -o test -Dtest=SlowConsumerTest          # REJECT / DROP_OLDEST / DISCONNECT + 隔离
mvn -o test -Dtest=RestTransportIntegrationTest # HTTP 端到端错误码

# 启动服务后用 curl 验证（见第 5 节）
mvn -o spring-boot:run
```

### 测试覆盖与可观测性

- `ReconnectRedeliveryTest`
  - 有序投递；ACK 后断线重连只从 `committed+1` 续传，已确认消息不重复；
  - 断线前在途消息重连为 `RETRY`，断线后新消息为 `FIRST`；
  - 未确认消息在 ACK 超时后重投，ACK 后不再交付；旧连接句柄代际失效。
- `AuthorizationTest`：未认证、发布越权、订阅越权、未知主题、畸形/超限报文、冒名/畸形续传令牌、会话冲突、越权操作他人会话、未订阅 ACK 等，错误码逐一断言。
- `SlowConsumerTest`：三种策略分别验证，且证明慢订阅不拖垮健康订阅、发布线程不被无限阻塞、缓冲有界；摘除后会话存活、可重新订阅/重连续传。
- `RestTransportIntegrationTest`：经真实 Spring MVC 验证错误码、HTTP 状态与会话生命周期。

用例日志统一打印**会话标识、消息序号、确认位点与判定依据**，例如：

```
[判定] session=s-2-.. 收到消息 seq=4 投递类型=RETRY 第2次投递 当前确认位点=3 判定依据=按主题顺序交付
[判定] session=s-3-.. 事件=PUBLISH_REJECTED seq=5 判定依据=错误码=SUBSCRIBER_BACKPRESSURE_REJECTED ...
[判定] session=s-2-.. 事件=DROPPED_OLDEST seq=1 判定依据=积压达上限 4，按 DROP_OLDEST ...
[判定] 操作被拒绝 错误码=PUBLISH_FORBIDDEN 原因='身份 observer 无权向主题 robot.cmd 发布' ...
```

---

## 8. 代码结构

```
bus/
├── api/            MessageBus / BusSession 对外接口
├── model/          Message、SessionEvent、TopicConfig、Principal、ResumeToken、枚举
├── error/          BusErrorCode（统一错误码）、BusException
├── auth/           AuthorizationService 及配置驱动实现（通配授权）
├── config/         BusProperties（robot.bus.*）、BusConfig（Spring 装配）
├── engine/         LocalMessageBus：有序投递/ACK去重/超时重投/背压/会话代际
└── transport/      BusRestController：HTTP 传输与统一错误响应
```

核心实现集中在 `engine/LocalMessageBus.java`：主题锁保护各订阅状态，事件入队为唯一的锁外操作并有界等待，从而在保证一致性的同时隔离慢消费者。
