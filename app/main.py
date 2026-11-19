"""FastAPI 入口：探测 API + 本地回显端 + 遥测合包网关。

- ``GET  /``                       服务简介
- ``GET  /echo/config``            查看回显端当前配置与 epoch
- ``PUT  /echo/config``            设置丢弃阈值 / 断联（校验失败保留原状态）
- ``POST /probe``                  发起一次探测（默认范围 576~1400 字节）
- ``GET  /probe/{id}``             查询历史探测结论
- ``GET  /probes``                 历史探测结论列表
- ``POST /telemetry/samples``      受理一条遥测样本（按设备合包，超限拒收）
- ``GET  /telemetry/config``       查看合包负载上限与发送/待发统计
- ``PUT  /telemetry/config``       调整负载上限（256~1400，含封装开销）
- ``GET  /telemetry/received``     本地接收器从真实 UDP 数据报解出的记录

交互式文档见 ``/docs``。
"""
from __future__ import annotations

import asyncio
import os
import socket
from collections import OrderedDict
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI, HTTPException, Request

from .echo_server import EchoConfig, EchoProtocol, EchoState
from .netutil import set_dont_fragment
from .prober import ProbeRequest, ProbeResult, Prober
from .telemetry import (
    DEFAULT_PAYLOAD_LIMIT,
    FLUSH_INTERVAL_S,
    Batcher,
    BatcherStats,
    LimitAdjustmentError,
    OversizeSample,
    ReceivedRecord,
    ReceiverStats,
    SampleAck,
    SampleIn,
    SenderProtocol,
    TelemetryConfigIn,
    TelemetryReceiver,
)

ECHO_HOST = os.environ.get("ECHO_HOST", "0.0.0.0")
ECHO_PORT = int(os.environ.get("ECHO_PORT", "9999"))
PROBE_TARGET_HOST = os.environ.get("PROBE_TARGET_HOST", "127.0.0.1")
# 遥测合包：发送目标（本地接收器）与本地接收器监听地址
TELEMETRY_HOST = os.environ.get("TELEMETRY_HOST", "0.0.0.0")
TELEMETRY_PORT = int(os.environ.get("TELEMETRY_PORT", "9998"))
TELEMETRY_TARGET_HOST = os.environ.get("TELEMETRY_TARGET_HOST", "127.0.0.1")

MAX_RESULTS = 200  # 内存中保留的探测结论条数上限


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    state = EchoState()

    # 回显端 socket：设置 DF，回包同样禁止 IP 分片
    echo_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    echo_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    set_dont_fragment(echo_sock)
    echo_sock.bind((ECHO_HOST, ECHO_PORT))
    echo_sock.setblocking(False)

    loop = asyncio.get_running_loop()
    echo_protocol = EchoProtocol(state)
    echo_transport, _ = await loop.create_datagram_endpoint(
        lambda: echo_protocol, sock=echo_sock
    )

    # 遥测本地接收器：独立 UDP 端口，解析真实数据报
    recv_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    recv_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    recv_sock.bind((TELEMETRY_HOST, TELEMETRY_PORT))
    recv_sock.setblocking(False)
    receiver = TelemetryReceiver()
    recv_transport, _ = await loop.create_datagram_endpoint(
        lambda: receiver, sock=recv_sock
    )

    # 遥测发送端：合包后发往本地接收器
    sender_transport, sender_proto = await loop.create_datagram_endpoint(
        SenderProtocol, remote_addr=(TELEMETRY_TARGET_HOST, TELEMETRY_PORT)
    )
    batcher = Batcher(
        sender_transport,
        (TELEMETRY_TARGET_HOST, TELEMETRY_PORT),
        payload_limit=DEFAULT_PAYLOAD_LIMIT,
    )

    # 后台刷新：包内最早样本等满 2 秒即发包
    flusher_task = asyncio.create_task(_flusher_loop(batcher))

    app.state.echo_state = state
    app.state.echo_protocol = echo_protocol
    app.state.prober = Prober(state, (PROBE_TARGET_HOST, ECHO_PORT))
    app.state.results = OrderedDict()  # probe_id -> ProbeResult
    app.state.batcher = batcher
    app.state.receiver = receiver
    app.state.sender_protocol = sender_proto
    try:
        yield
    finally:
        flusher_task.cancel()
        recv_transport.close()
        sender_transport.close()
        echo_transport.close()


async def _flusher_loop(batcher: Batcher) -> None:
    while True:
        try:
            await asyncio.sleep(FLUSH_INTERVAL_S)
            await batcher.flush_due()
        except asyncio.CancelledError:
            raise
        except Exception:  # pragma: no cover - 刷新异常不应杀死服务
            pass


app = FastAPI(
    title="遥测网关：UDP 最大负载探测 + 高频样本合包",
    version="1.1.0",
    description=(
        "## 探测能力\n"
        "小包能到回显端、大包却超时？用本服务确认遥测网关可用的最大 UDP 负载。\n\n"
        "- 探测报文带 DF 位（禁止 IP 分片）与本次探测编号，只有对应回包才算成功；\n"
        "- 大包超时/发送失败先用小包复核，小包也失败则报告链路不确定；\n"
        "- 探测期间改动回显端配置会使本次结论作废；\n"
        "- 在下方 `PUT /echo/config` 设置丢弃阈值或断联，再 `POST /probe` 验证。\n\n"
        "## 遥测合包\n"
        "高频短样本不再一条一个 UDP 包：发送端按设备各自合包，按受理顺序装入完整"
        "样本；包满、下一条放不下或包内最早样本已等 2 秒时发包。\n\n"
        "- `POST /telemetry/samples`：填设备号与十六进制样本即可模拟遥测设备"
        "（交替使用两个设备号即模拟两台设备）；单条样本超过当前负载上限直接拒收；\n"
        "- `PUT /telemetry/config`：调整 UDP 负载上限（256~1400 字节，含封装开销），"
        "调低后待发样本按新上限重分包，保留受理顺序与等待起点；若有待发样本单独"
        "成包也超限则拒绝调整；\n"
        "- `GET /telemetry/received`：查看本地接收器从**实际 UDP 数据报**解出的"
        "设备号、每包负载字节数与样本原文。"
    ),
    lifespan=lifespan,
)


@app.get("/", summary="服务简介")
async def index() -> dict:
    return {
        "service": "telemetry-gateway",
        "docs": "/docs",
        "endpoints": {
            "config": ["GET /echo/config", "PUT /echo/config"],
            "probe": ["POST /probe", "GET /probes", "GET /probe/{probe_id}"],
            "telemetry": [
                "POST /telemetry/samples",
                "GET /telemetry/config",
                "PUT /telemetry/config",
                "GET /telemetry/received",
            ],
        },
    }


@app.get("/echo/config", summary="查看回显端配置")
async def get_config(request: Request) -> dict:
    state: EchoState = request.app.state.echo_state
    proto: EchoProtocol = request.app.state.echo_protocol
    return {
        "epoch": state.epoch,
        "config": state.config,
        "stats": {"echoed": proto.echoed, "dropped": proto.dropped},
    }


@app.put("/echo/config", summary="设置丢弃阈值或断联")
async def put_config(cfg: EchoConfig, request: Request) -> dict:
    # 入参先经 Pydantic 校验；校验失败直接 422，不会进入这里，原状态保持不变
    state: EchoState = request.app.state.echo_state
    epoch = state.apply(cfg)
    return {"epoch": epoch, "config": state.config}


@app.post("/probe", response_model=ProbeResult, summary="发起一次最大负载探测")
async def run_probe(req: ProbeRequest, request: Request) -> ProbeResult:
    prober: Prober = request.app.state.prober
    try:
        result = await prober.probe(req)
    except OSError as exc:
        # 目标地址无法解析/不可达等，探测未能发起
        raise HTTPException(status_code=400, detail=f"无法发起探测: {exc}") from exc
    results: OrderedDict[str, ProbeResult] = request.app.state.results
    results[result.probe_id] = result
    while len(results) > MAX_RESULTS:
        results.popitem(last=False)
    return result


@app.get("/probe/{probe_id}", response_model=ProbeResult, summary="查询历史探测结论")
async def get_probe(probe_id: str, request: Request) -> ProbeResult:
    results: OrderedDict[str, ProbeResult] = request.app.state.results
    try:
        return results[probe_id]
    except KeyError:
        raise HTTPException(status_code=404, detail="probe_id 不存在或已被淘汰") from None


@app.get("/probes", summary="历史探测结论列表")
async def list_probes(request: Request) -> list[dict]:
    results: OrderedDict[str, ProbeResult] = request.app.state.results
    return [
        {
            "probe_id": r.probe_id,
            "status": r.status,
            "max_confirmed_size": r.max_confirmed_size,
            "reason": r.reason,
            "finished_at": r.finished_at,
        }
        for r in reversed(list(results.values()))
    ]


# ---------------------------------------------------------------- 遥测合包


@app.post(
    "/telemetry/samples",
    response_model=SampleAck,
    summary="受理一条遥测样本（按设备合包，单条超限拒收）",
)
async def submit_sample(req: SampleIn, request: Request) -> SampleAck:
    batcher: Batcher = request.app.state.batcher
    try:
        return await batcher.submit(req.device_id, req.sample_hex)
    except OversizeSample as exc:
        # 单条样本（含封装开销）超过当前负载上限：拒收，不进入任何待发队列
        raise HTTPException(
            status_code=422,
            detail=(
                f"样本超限被拒收：{exc.sample_bytes} 字节样本单独成包需 "
                f"{exc.packet_bytes} 字节（含封装开销），"
                f"超过当前 UDP 负载上限 {exc.limit} 字节"
            ),
        ) from exc


@app.get(
    "/telemetry/config",
    summary="查看合包负载上限与发送/待发统计",
)
async def get_telemetry_config(request: Request) -> dict:
    batcher: Batcher = request.app.state.batcher
    sender: SenderProtocol = request.app.state.sender_protocol
    stats = await batcher.stats()
    return {
        "payload_limit": stats.payload_limit,
        "payload_limit_range": [256, 1400],
        "max_age_seconds": 2.0,
        "target": f"{TELEMETRY_TARGET_HOST}:{TELEMETRY_PORT}",
        "stats": stats.model_dump(),
        "sender": {"send_errors": sender.send_errors},
    }


@app.put(
    "/telemetry/config",
    summary="调整 UDP 负载上限（待发样本重分包；不可行则拒绝）",
)
async def put_telemetry_config(cfg: TelemetryConfigIn, request: Request) -> dict:
    batcher: Batcher = request.app.state.batcher
    try:
        stats: BatcherStats = await batcher.set_payload_limit(cfg.payload_limit)
    except LimitAdjustmentError as exc:
        # 有待发样本单独成包也超过新上限：拒绝调整，继续按旧上限发送
        current = await batcher.stats()
        raise HTTPException(
            status_code=409,
            detail={
                "error": "payload_limit_adjustment_rejected",
                "message": str(exc),
                "rejected_limit": cfg.payload_limit,
                "current_payload_limit": current.payload_limit,
            },
        ) from exc
    return {
        "payload_limit": stats.payload_limit,
        "repacked": True,
        "stats": stats.model_dump(),
    }


@app.get(
    "/telemetry/received",
    summary="本地接收器从实际 UDP 数据报解出的接收记录",
)
async def list_received(request: Request) -> dict:
    receiver: TelemetryReceiver = request.app.state.receiver
    records: list[ReceivedRecord] = receiver.list_records()
    recv_stats: ReceiverStats = receiver.stats()
    return {
        "listen": f"{TELEMETRY_HOST}:{TELEMETRY_PORT}/udp",
        "stats": recv_stats.model_dump(),
        "records": [r.model_dump() for r in reversed(records)],
    }
