# robot-pubsub-bus-java

面向机器人指令下发与遥测上报的本地发布/订阅消息总线（Spring Boot 工程）。
核心实现位于 `com.github.xingjianluo.robotpubsubbusjava.bus` 包，不依赖任何外部服务。

## 投递语义

- **至少一次（at-least-once）**：消息在确认前可能被重复投递，消费者需按主题内序号 `seq` 自行去重。
- **会话内有序**：同一订阅按主题内单调递增序号 `seq` 顺序投递。
- **首次投递与重投可区分**：`Delivery.redelivered` 标志与 `Delivery.attempt`（投递次数，从 1 开始）
  明确区分首次投递与重投。
- **确认机制**：`ack(sessionId, topic, seq)` 为累积确认，确认位点推进到 `seq`（含），
  已确认消息不再投递。投递后 `ack-timeout-millis` 内未确认的消息将被重投。

## 重连与去重规则

- `disconnect` 断开会话时保留全部订阅状态与确认位点，并将未确认消息置为待重投。
- `reconnect` 以原会话标识恢复会话，从已确认位点之后继续投递：
  未确认消息以 `redelivered=true` 优先重投，随后按序投递新消息。
- 已确认位点之前（含位点）的消息绝不重复交付；重连未知会话以 `SESSION_NOT_FOUND` 拒绝。

## 背压与资源上限策略（慢消费者）

- 每个订阅的积压队列有界（`bus.max-backlog-per-subscription`），不存在无界内存增长。
- 积压满时按**丢弃最旧**策略处理，并产生 `DropEvent(reason=BACKLOG_OVERFLOW)`，对客户端可见。
- 连续丢弃达到 `bus.max-drops-before-disconnect` 次后，该订阅被判定为慢消费者并断开，
  产生 `DropEvent(reason=SLOW_CONSUMER_EVICTED)`；此后对该订阅的拉取以 `SUBSCRIPTION_EVICTED` 显式拒绝。
- 丢弃与断开是**按订阅隔离**的：一个慢消费者不会阻塞或拖垮其他订阅者。
- 被拒绝、被丢弃、被断开三者的原因分别由 `ErrorCode`、`DropReason.BACKLOG_OVERFLOW`、
  `DropReason.SLOW_CONSUMER_EVICTED` 区分。

## 错误分类

所有失败以 `BusException` 携带 `ErrorCode` 显式抛出，不静默断开、不静默丢消息：

| 错误码 | 含义 |
| --- | --- |
| `UNKNOWN_TOPIC` | 主题未在服务端注册 |
| `UNAUTHORIZED_PUBLISH` | 会话身份未被授权向该主题发布 |
| `UNAUTHORIZED_SUBSCRIBE` | 会话身份未被授权订阅该主题 |
| `MALFORMED_MESSAGE` | 空报文或结构非法 |
| `MESSAGE_TOO_LARGE` | 报文超过 `bus.max-payload-bytes` 上限 |
| `SESSION_NOT_FOUND` | 未知会话（如重连携带无效会话标识） |
| `SUBSCRIPTION_NOT_FOUND` | 订阅不存在 |
| `SUBSCRIPTION_EVICTED` | 订阅已因慢消费者策略被断开 |

授权基于服务端确认的会话身份 `SessionIdentity`（发布/订阅主题集合，`"*"` 表示全部主题）。

## 配置项（application.properties）

| 键 | 默认值 | 含义 |
| --- | --- | --- |
| `bus.max-backlog-per-subscription` | 100 | 单订阅积压上限 |
| `bus.ack-timeout-millis` | 5000 | 确认超时，超时重投 |
| `bus.max-payload-bytes` | 65536 | 单条报文字节上限 |
| `bus.max-drops-before-disconnect` | 3 | 连续丢弃断开阈值 |
| `bus.topics` | robot.commands,robot.telemetry | 启动时注册的主题 |

## 本地验证

不依赖外部服务，直接运行：

```bash
mvn test
```

测试覆盖四类场景，日志打印会话标识、消息序号、确认位点与判定依据：

- `ReconnectResumeTest`：断线重连后从确认位点续传，已确认消息不重复交付；
- `RedeliveryTest`：确认超时后重投，`redelivered`/`attempt` 可区分首投与重投；
- `AuthorizationTest`：越权发布/订阅、未知主题、畸形/超限报文、未知会话的错误码可区分；
- `SlowConsumerTest`：积压上限、丢弃最旧、慢消费者断开及对其他订阅者的隔离。
