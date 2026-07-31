# Shiba Ops Dashboard

独立的 React/Vite 运维看板。它不会从浏览器直接连接 PostgreSQL，而是读取一个只读的观测 API。

## Run

```bash
bun install
cp .env.example .env
bun run api:dev
```

另开一个终端启动前端：

```bash
bun run dev
```

默认打开 `http://localhost:4173`。Vite 会把 `/api` 请求代理到
`http://localhost:8787`。开发模式下如果观测 API 不可用，页面会明确显示
`DEMO FEED` 并使用内置拓扑 fixture，方便先审查交互和视觉。

生产构建默认不会用 Demo 数据掩盖 API 故障，而是显示连接错误和重试入口。只有在明确需要演示时才启用：

```bash
VITE_ENABLE_DEMO_FALLBACK=true bun run dev
```

配置真实 API：

```bash
VITE_OBSERVABILITY_API=http://localhost:8787/api/observability/snapshot bun run dev
```

API 也可以单独运行：

```bash
DATABASE_URL=postgres://postgres:shiba@127.0.0.1:5432/shiba bun run api
curl http://localhost:8787/healthz
curl http://localhost:8787/api/observability/snapshot
```

如果 API 使用了非默认端口，同时设置 `OBSERVABILITY_PORT` 和
`VITE_OBSERVABILITY_API`，或让 Vite 读取同一个 `.env`：

```bash
OBSERVABILITY_PORT=8878 \
VITE_OBSERVABILITY_API=http://localhost:8878/api/observability/snapshot \
bun run dev
```

## API contract

`GET /api/observability/snapshot` 返回一个 JSON snapshot，字段形状对应
`src/types.ts` 中的 `DashboardSnapshot`：

- `runtime`：Runtime、slot、WAL 和 heartbeat 状态；
- `pipeline`：系统级节点与边；
- `dataflows`：每个 MV 的摘要和健康状态；
- `dags`：以 dataflow id 为 key 的节点/边图；
- `alerts`：当前告警；
- `history`：用于小型趋势图的最近采样点。

生产 API 可以由 Bun/Elysia、Rust sidecar 或现有平台服务提供。数据库侧对应
`shiba.runtime_status()`、`shiba.runtime_metrics()`、`shiba.dataflow_status()`，
本仓库内置的 Bun API 会额外调用 `shiba.explain_dataflow()` 生成 stage-level
graph，并在内存中保留最近 48 次采样作为趋势数据。API 默认只监听
`127.0.0.1:8787`；部署到反向代理或容器时再设置 `OBSERVABILITY_HOST`。

API 本身不提供登录页。默认回环监听适合本机部署；如果设置为
`0.0.0.0`，必须放在已有认证、TLS 和网络访问控制的反向代理后面。数据库
账号只需要执行观测函数，并对希望展示的 Shiba 结果表拥有 `SELECT` 权限。
