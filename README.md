# 遥测网关：UDP 最大负载探测 + 高频样本合包

本服务包含两部分能力：

1. **UDP 最大负载探测**：小包能到回显端、大包却超时？为值班员确认遥测网关
   **可用的最大 UDP 负载**，默认探测范围 **576～1400 字节**，二分搜索收敛；
2. **遥测合包网关**：高频短样本不再一条一个 UDP 包，发送端**按设备各自合包**，
   按受理顺序装入完整样本，包满/放不下/等待 2 秒即发，负载上限可在 256～1400
   字节间动态调整。

FastAPI + asyncio 实现，所有操作都能在 `/docs` 交互完成。

## 快速开始

### Docker Compose（推荐）

```bash
docker compose up --build
# 打开 http://localhost:8000/docs
```

### 本地运行

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

### 运行测试

```bash
pip install -r requirements-dev.txt
pytest tests/ -v
```

## 遥测合包（/docs 内即可操作）

### 模拟设备上报：`POST /telemetry/samples`

```json
{"device_id": "dev-1", "sample_hex": "0102ab"}
```

- 交替使用两个设备号（如 `dev-1` / `dev-2`）即可模拟两台设备，设备各自独立合包；
- `sample_hex` 为偶数位十六进制样本原文，按受理顺序完整装入；
- **单条样本（含封装开销）超过当前负载上限 → 直接 422 拒收**，不进入待发队列。

### 发包时机（每台设备独立判断）

1. **包满**：再装下一条就会超过负载上限 → 当前包立即发出，下一条进新包；
2. **等待超时**：包内**最早的样本**已等待 2 秒（后到样本不重置起点）→ 后台任务发包。

### 调整 UDP 负载上限：`PUT /telemetry/config`

```json
{"payload_limit": 512}
```

- 合法范围 **256～1400 字节**，且**计入封装开销**（合包头与每条样本的长度前缀）；
- 调低成功后，待发样本**按新上限重新分包**，原受理顺序与等待起点保持不变，
  重新分包后装满的包立即发出；
- 若**任一待发样本单独成包也超过新上限** → 返回 **409 拒绝调整**，
  继续按旧上限发送，队列原样保留。

### 查看真实接收结果：`GET /telemetry/received`

本地接收器从**实际 UDP 数据报**解码，返回最近 200 条接收记录：

| 字段 | 含义 |
|---|---|
| `device_id` | 数据报携带的设备号 |
| `packet_bytes` | 本 UDP 数据报的实际负载字节数 |
| `samples_hex` | 包内样本原文（按包内顺序） |
| `received_at` | 接收时刻（UTC） |

`GET /telemetry/config` 同时返回发送端统计：累计受理/拒收/发包数、各设备待发条数、
最老待发样本年龄等。

### 合包数据报线格式（大端）

```
| magic  | dev_len | device_id  | 每条样本：sample_len(2B) + sample |
| "TEL1" | uint8   | UTF-8 字节 | uint16 大端长度 + 样本原始字节     |
```

任何字段不自洽的数据报在接收端计入 `malformed_datagrams_total`，不会入库。

## 探测能力（原有 API，照常可用）

### 值班流程

1. **设置回显端行为**：`PUT /echo/config`
   - `{"mode": "normal", "drop_threshold": 1000}` — UDP 负载超过 1000 字节即丢弃；
   - `{"mode": "disconnect", "drop_threshold": null}` — 断联，丢弃一切报文；
   - 入参校验失败返回 422，**原配置保持不变**。
2. **发起探测**：`POST /probe`（Body 可全部缺省）

   ```json
   {"min_size": 576, "max_size": 1400, "timeout_ms": 500, "small_size": 64}
   ```
3. **读取结论**：

   | `status` | 含义 | `max_confirmed_size` |
   |---|---|---|
   | `ok` | 已确认最大可用负载 | 确认值（等于 `max_size` 时 `reached_range_max=true`，实际上限可能更高） |
   | `below_range` | 链路正常，但范围下限都确认不了 | `null` |
   | `undetermined` | 大包失败后小包复核也失败，**链路不确定** | 此前已确认的下界（无则 `null`），**绝不把故障当作尺寸上限** |
   | `invalidated` | 探测期间回显端配置被修改，**本次结论作废** | `null` |

   `attempts[]` 列出每次探测（`probe`）与小包复核（`link-verify`）的尺寸、结果与 RTT。
4. 历史结论：`GET /probes`、`GET /probe/{probe_id}`（内存保留最近 200 条）。

### 判定规则与需求对应

- **禁止 IP 分片**：探测与回显 socket 均设置 `IP_MTU_DISCOVER/IP_PMTUDISC_DO`（DF=1），
  结果中 `df_enabled` 如实上报；超过路径 MTU 时发送直接失败（EMSGSIZE），按发送失败处理。
- **报文带本次探测编号**：每次运行生成 UUID，连同序号写入 28 字节报文头
  （`magic | UUID | seq | size | 填充`）；只有 magic/编号/序号/长度**全部对应**的回包才算成功。
- **大包失败小包复核**：大包超时或发送失败 → 立即用 `small_size`（默认 64B）复核链路；
  小包通 = 确为尺寸受限，继续二分；小包也不通 = `undetermined`，故障不记为尺寸上限。
- **配置改变结论作废**：回显端配置带单调递增的 `epoch`，探测开始/每步/结束比对，
  不一致即 `invalidated`。
- **并发与迟到回包隔离**：每次探测运行使用独立临时端口 socket 并 connect 到回显端，
  其他运行的回包在内核层就被过滤；迟到的本次回包按序号拒收并计入 `stray_replies`。
- **配置失败保留原状态**：配置先经 Pydantic 校验，通过才原子替换并递增 epoch。

## 环境变量

| 变量 | 默认 | 说明 |
|---|---|---|
| `ECHO_HOST` | `0.0.0.0` | 回显端监听地址 |
| `ECHO_PORT` | `9999` | 回显端 UDP 端口 |
| `PROBE_TARGET_HOST` | `127.0.0.1` | 探测默认目标（`POST /probe` 可用 `target_host/target_port` 覆盖，指向真实网关） |
| `TELEMETRY_HOST` | `0.0.0.0` | 遥测接收器监听地址 |
| `TELEMETRY_PORT` | `9998` | 遥测接收器 UDP 端口 |
| `TELEMETRY_TARGET_HOST` | `127.0.0.1` | 合包发送目标（容器内默认指向本地接收器） |

## 项目结构

```
app/
  main.py        # FastAPI 路由与生命周期（探测 + 遥测）
  echo_server.py # 本地回显端（阈值丢弃/断联，epoch）
  prober.py      # 二分探测引擎（DF、探测编号、小包复核、作废判定）
  packets.py     # 探测报文编解码
  telemetry.py   # 合包发送器（按设备队列/2 秒刷新/动态上限）、本地接收器与线格式
  netutil.py     # DF 位设置
tests/test_api.py        # 探测与回显端测试
tests/test_telemetry.py  # 合包、上限调整、接收器测试
Dockerfile / docker-compose.yml
```
