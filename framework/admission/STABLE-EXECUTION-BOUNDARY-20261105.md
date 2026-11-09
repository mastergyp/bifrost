# 稳定执行边界：当前资格与最小迁移

本次是自有故障资格及迁移边界，未改变运行代码、生产部署、原Key、权限、额度或物理容量，也未声明PG硬准入已被移除。

## 已修正的旧生命周期行为

此前真实Bifrost Core会在managed短路stream的调用者消失后继续读取原Producer流。Producer的Background-owned上下文不会随Gateway调用者取消，虽然可以等待最终usage，却会让已经没有消费者的vLLM生成继续占用GPU。

现在managed operation仍独立于60秒admission deadline，但会监听原调用者的显式 `context.Canceled`。下游断开时它取消自有上下文，关闭Unix inference body并把取消继续传给原生provider；普通deadline到期不触发该联动，因此不会截断长推理。取消前已经观察到的usage仍进入原post-hook，未到达的终结usage不会被虚构。

`TestManagedCoreCancelsNativeRequestWhenCallerDisconnects` 使用实际Core、实际Producer、真实Unix HTTP sender与SSE decoder，分别覆盖Chat和Responses：

- 自有sender发出片段后等待终结；无真实GPU或用户内容。
- 下游读首片后取消，sender必须观察到原HTTP request context取消。
- Producer permit归零并可复用；没有虚构未收到的终结usage。
- 实际sender调用仅1次，没有重放推理。

`TestManagedOwnedOperationIgnoresCallerDeadline` 另行固定长推理边界。私有managed sender无法由通用provider harness配置，因此不增加或运行付费harness case。

## 真正的Gateway生命周期耦合点

Producer当前实际HTTP只发往固定Unix `/inference/v1/chat/completions`、`/inference/v1/responses`。GPU HTTP sender在Gateway进程中的Rust `Dispatcher`；目标准备、参数校验、物理claim和终结落库也位于现有Rust控制端。

因此，单把Producer改成直接TCP请求vLLM会绕过以下已部署契约，不能作为快速迁移：

- 原主体/会话/请求体digest/注册namespace与contract一致性。
- 目标模型权限、原显式参数、图片/工具/上下文资格和实际选中funding。
- prepared→dispatch原子claim与nonce重放保护。
- backend固定地址、实际模型Key只从受控私有配置加载。
- 流式真实终结识别及不把断开/超时冒充终结。

## 最小可交付迁移

第一步把已有Rust控制端和Dispatcher构造器抽到独立、稳定执行的进程，与现有Bifrost共同作为执行边界运行；不新增另一套调度算法，不重写现有wire协议。Gateway只承担原身份/额度预留/用户连接与异步交付。Bifrost使用相同受限UDS和原prepared/inference接口；对外API、原model Key和funding/post-hook语义保持。

| 边界 | 最小契约 |
| --- | --- |
| 计划与准入 | health/prepare/enqueue/poll/cancel原协议、绝对60秒deadline、namespace/contract拒绝漂移 |
| 执行 | 原operation/owner/digest、独占claim、固定backend、禁止未知HTTP结果重放 |
| 生命周期 | Gateway退出不关闭执行服务UDS或runtime；新Gateway实例仍访问同一稳定服务 |
| 配置 | 稳定执行服务独立加载固定版本模型契约与私有secret引用；热更不修改活动请求对象 |
| 回调 | caller显式取消同时终止native HTTP流；仅已观察证据进入原Core post-hooks，不另起第二条财务流水 |
| 关闭 | 先拒新执行、等待已拥有任务/回调；未知任务隔离到所属实例，不恢复为“无在途” |

迁移资格应保留旧Gateway连接的SSE，启动新Gateway并证明后者可接入，随后正常退出旧Gateway；稳定执行服务PID/UDS/native任务不变。把旧Gateway固定UDS复制到两个进程上不能算滚动升级。

第二步才改变PG物理准入职责：引擎运行/KV保护及已资格总在途阀兜底，执行层的实时容量与账务完成分离。必须显式引入新执行代次/能力标记；旧unknown记录保留为恢复事实，不能直接TTL删除或冒充物理任务已结束。旧包回退需要明确新旧协议资格，不能静默恢复旧数据库。

如果后续原生迁入Go/Bifrost，可替换内部executor实现，但上述契约仍须逐项保留。当前测试证明Core取消会穿透Unix inference hop且不受普通deadline误触发，不证明执行进程已经迁出Gateway或单实例自动恢复已经完成。
