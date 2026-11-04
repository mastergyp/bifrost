# 流读取阶段与构建容量保护

2026-11-04：用户继续授权P0收尾，服务可用优先。当前线上7ad/6522/CID1bc；第一轮读取阶段代码已部署，但owned诊断归属漏项仍待后续候选。不能把源码或本机测试当部署完成。

已确认OCR目录约9分钟净增757645312bytes，接近同期主机可用空间下降；这是当前主要增长来源，不应只归因Docker。线上Doc Engine72h清理已实际启用、最近清11任务且旧72h行0；本地OCR源码有用户未提交工作，与线上新增retention字段不同，未重复修改/部署/删除任务。模型DS原生瞬时28running/0waiting、GPU99–100%，代理buffering off/read timeout1800s，不能因此认定长尾原因。

## 读取诊断候选

在原`bifrost_stream_read_failure`上只允许实际RoutingInfo.Provider为gpu0/1/2时记录selected_provider；不记录原模型名、Key、请求ID、错误文本或用户正文。原owned context通过父快照读取实际选中目标，不能把PrimaryProvider当结果。IdleReader包内私有类型键只保存原子started/reading/progress/timeout-phase，发生原idle错误时记录有限`read_blocked`/`between_reads`/`not_started`和`none`/`some`。这是超时瞬间观察，不等同网络/GPU/背压根因；没有更改timer、关闭、错误对象、SSE、retry/fallback、额度/计量、物理释放和原120/600秒设置。

真实io.Pipe两场景red缺字段，green覆盖父上下文/源gpu0与选中gpu1、无数据blocked/有数据后between、未知provider/model不出日志。原utils整包34.389s/admission0.965s通过。仅日志和构建流程，没有wire-visible变化，按AGENTS豁免provider-harness；未执行付费推理或全套UI。

## 构建增长预防

原Dockerfile先COPY全部Go源码再download，源代码任意变化都会复制依赖层。新布局先显式COPY15个模块的go.mod/sum、建立同样workspace并download，后续build-source才COPY运行源码；原network-none admission/utils/server测试、main及probe/static/runtime元数据保持。测试将来新增module必须更新manifest COPY闭包。

真实本机network-none Docker夹具（合成go，仅验证COPY/cache失效）证实旧布局runtime-only edit改变dependency image、新布局保留同image；精确4owned标签清理。**不是完整真实Go编译证明**。本机只含manifest的真实go download/offline资格因444个缓存缺失而阻止，不能报成功；固定Linux构建必须实际跑原资格。

managed-build在intent/构建前检查/data可用空间，保留max(总容量10%,24GiB)+12GiB构建头寸；缺空间明确拒绝，无自动清理和业务重启。全程同一专用非阻塞build锁防本helper并发抢同一余量。这是构建前保护，不是磁盘硬配额，也不协调其他项目任意写入；当前运行包及旧root脚本没有被替换，后续固定源码构建才采用。

[本机证据](evidence/stream-phase-build-local-20261104.json)。下一步固定Linux包、核对原actor及同当前数据回退后再考虑独立Bifrost发布；不运行旧固定CID联合发布器，不重发任何旧intent。

## 17:02 Linux包完成，未激活

固定7adf4fc/image6522c886/main SHA c1e8cb18，原`sw-bifrost-stream-phase-build-7adf4fc-r1` MainPID0/success0。admission0.916s/utils1.213s/server0.041s实际通过，main和原8官方runtime字段验证完成；五原服务足迹保持。构建前可用60420657152bytes、服务余量42160847257、头寸12884901888；Linux网络下载补齐了本机offline未覆盖资格。原本机SSH等待240秒超时，但服务器原unit持续构建并最终成功；不重发、不重建、没有生产重启。后续分发systemd-run用no-block，未知只观察原unit。[Linux制品](evidence/stream-phase-build-linux-20261104.json)。

下一步manifest固定新APP7/image6522/source9-CID722/image d709/purpose stream-phase；原数据回退仍用同协议当前状态，绝不还原旧快照或启动第二writer。先新ops两owned Docker正向/失败回退资格与stopped prepare，再经原ready/authority/config守卫考虑激活，不使用历史68/c365常量。不做GPU故障或付费模型重复测试。

## 17:26真实owned链路修正与本机构建候选

真实Producer创建Background-owned而非从请求ctx继承；第一轮上线三条idle没有新归属字段。现Chat/Responses实际Unix/HTTP/OpenAI链两条red缺字段，green用选中/funding校验后的独立typed私有诊断键，不写RoutingInfo、不改grant/cancel/wire，未知目标不回退源标签。admission整包0.969s、定向core0.135s通过。

生产空闲52.756GB低于55.046GB编译门槛；不降低12GiB编译保护、不再在主机生成Go/npm层。新增本机Docker Desktop固定Git/Linux-amd64全资格编译，实际cgroup夹具已证明每step2CPU/4GiB。服务端仅组装静态AMD64 main，固定base/8原runtime字段/同RootFS前缀、包大小与SHA和Git闭包验证；严格三文件tar拒绝多路径/软链/任意RUN/源或hash错误/动态解释器。静态COPY工作量使用10%/24GiB服务余量+3GiB单独组装头寸，**没有绕过完整编译12GiB保护**。本机纯包/预算13测试通过，真正本机Linux完整编译和生产组装尚未执行，不提前声明通过。

用户最新要求优先清理生产盘，候选先提交保存；生产当前仍7ad，数据库/用户文档/回退资料不动。后续按精确旧构建cache计划受控处理，再继续本机编译及真实字段验收。[候选证据](evidence/owned-diagnostic-local-build-20261104.json)。
