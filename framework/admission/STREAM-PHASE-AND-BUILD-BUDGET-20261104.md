# 流读取阶段与构建容量保护

2026-11-04：用户继续授权P0收尾，服务可用优先。当前线上仍9abd/d709/CID722，本文先记录候选，不把源码或本机测试当部署完成。

已确认OCR目录约9分钟净增757645312bytes，接近同期主机可用空间下降；这是当前主要增长来源，不应只归因Docker。线上Doc Engine72h清理已实际启用、最近清11任务且旧72h行0；本地OCR源码有用户未提交工作，与线上新增retention字段不同，未重复修改/部署/删除任务。模型DS原生瞬时28running/0waiting、GPU99–100%，代理buffering off/read timeout1800s，不能因此认定长尾原因。

## 读取诊断候选

在原`bifrost_stream_read_failure`上只允许实际RoutingInfo.Provider为gpu0/1/2时记录selected_provider；不记录原模型名、Key、请求ID、错误文本或用户正文。原owned context通过父快照读取实际选中目标，不能把PrimaryProvider当结果。IdleReader包内私有类型键只保存原子started/reading/progress/timeout-phase，发生原idle错误时记录有限`read_blocked`/`between_reads`/`not_started`和`none`/`some`。这是超时瞬间观察，不等同网络/GPU/背压根因；没有更改timer、关闭、错误对象、SSE、retry/fallback、额度/计量、物理释放和原120/600秒设置。

真实io.Pipe两场景red缺字段，green覆盖父上下文/源gpu0与选中gpu1、无数据blocked/有数据后between、未知provider/model不出日志。原utils整包34.389s/admission0.965s通过。仅日志和构建流程，没有wire-visible变化，按AGENTS豁免provider-harness；未执行付费推理或全套UI。

## 构建增长预防

原Dockerfile先COPY全部Go源码再download，源代码任意变化都会复制依赖层。新布局先显式COPY15个模块的go.mod/sum、建立同样workspace并download，后续build-source才COPY运行源码；原network-none admission/utils/server测试、main及probe/static/runtime元数据保持。测试将来新增module必须更新manifest COPY闭包。

真实本机network-none Docker夹具（合成go，仅验证COPY/cache失效）证实旧布局runtime-only edit改变dependency image、新布局保留同image；精确4owned标签清理。**不是完整真实Go编译证明**。本机只含manifest的真实go download/offline资格因444个缓存缺失而阻止，不能报成功；固定Linux构建必须实际跑原资格。

managed-build在intent/构建前检查/data可用空间，保留max(总容量10%,24GiB)+12GiB构建头寸；缺空间明确拒绝，无自动清理和业务重启。全程同一专用非阻塞build锁防本helper并发抢同一余量。这是构建前保护，不是磁盘硬配额，也不协调其他项目任意写入；当前运行包及旧root脚本没有被替换，后续固定源码构建才采用。

[本机证据](evidence/stream-phase-build-local-20261104.json)。下一步固定Linux包、核对原actor及同当前数据回退后再考虑独立Bifrost发布；不运行旧固定CID联合发布器，不重发任何旧intent。
