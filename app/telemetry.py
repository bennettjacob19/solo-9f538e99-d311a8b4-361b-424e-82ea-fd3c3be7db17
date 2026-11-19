"""遥测合包发送器与本地接收器。

设计要点（与需求一一对应）：

- 两台（或任意多台）设备各自维护独立的待发队列，按受理顺序装入完整样本，
  设备之间互不串包；
- 一条样本到达时：单条样本（含封装开销）超过当前负载上限 → 当场拒收；
  否则受理并尝试装入当前包；
- 发包时机（每设备独立判断）：
  1. 包满（再装下一条就会超过负载上限）→ 立即发；
  2. 包内最早的样本已等待 2 秒 → 后台刷新任务发包；
- 负载上限可调（256~1400 字节，含封装开销）。调低时先校验所有待发样本
  单独成包是否都放得下：任一放不下则拒绝调整、维持旧上限；通过后按新上限
  重新分包，样本受理顺序与等待起点（首次受理时刻）保持不变；
- 本地接收器从真实 UDP 数据报解码，记录设备号、每包负载字节数与样本原文。

合包数据报线格式（大端）::

    | 0..3   | 4       | 5..         | 之后每样本            |
    | magic  | dev_len | device_id   | sample_len(2B)+sample |
    | "TEL1" | uint8   | UTF-8 字节  | uint16 + 原始字节     |
"""
from __future__ import annotations

import asyncio
import time
from collections import deque
from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

# ---------------------------------------------------------------------------
# 常量与线格式
# ---------------------------------------------------------------------------

MAGIC = b"TEL1"
MIN_PAYLOAD_LIMIT = 256
MAX_PAYLOAD_LIMIT = 1400
DEFAULT_PAYLOAD_LIMIT = 1400
MAX_DEVICE_ID_BYTES = 32
FLUSH_INTERVAL_S = 0.05
MAX_AGE_S = 2.0
MAX_RECEIVED_RECORDS = 200

# 固定头：magic(4) + dev_len(1)
_HEADER_LEN = 5
_SAMPLE_LEN_BYTES = 2


class LimitAdjustmentError(Exception):
    """调整负载上限被拒绝（存在单独成包也超限的待发样本）。"""


def device_overhead(device_id: str) -> int:
    """该设备每个数据报的固定封装开销（不含样本自身）。"""
    return _HEADER_LEN + len(device_id.encode("utf-8"))


def single_sample_size(device_id: str, sample: bytes) -> int:
    """单条样本单独成包时的完整负载字节数（含全部封装开销）。"""
    return device_overhead(device_id) + _SAMPLE_LEN_BYTES + len(sample)


def build_datagram(device_id: str, samples: list[bytes]) -> bytes:
    """按线格式组装一个合包数据报。"""
    if not samples:
        raise ValueError("数据报至少包含一条样本")
    dev = device_id.encode("utf-8")
    if len(dev) > MAX_DEVICE_ID_BYTES:
        raise ValueError(f"设备号字节长度超过 {MAX_DEVICE_ID_BYTES}")
    parts: list[bytes] = [MAGIC, bytes((len(dev),)), dev]
    for sample in samples:
        if len(sample) > 0xFFFF:
            raise ValueError("单条样本长度超过 65535 字节")
        parts.append(len(sample).to_bytes(_SAMPLE_LEN_BYTES, "big"))
        parts.append(sample)
    return b"".join(parts)


def parse_datagram(data: bytes) -> Optional[tuple[str, list[bytes]]]:
    """解码真实 UDP 数据报；任何不自洽之处返回 None。"""
    if len(data) < _HEADER_LEN or data[:4] != MAGIC:
        return None
    dev_len = data[4]
    if dev_len == 0 or len(data) < _HEADER_LEN + dev_len:
        return None
    try:
        device_id = data[_HEADER_LEN : _HEADER_LEN + dev_len].decode("utf-8")
    except UnicodeDecodeError:
        return None
    pos = _HEADER_LEN + dev_len
    samples: list[bytes] = []
    while pos < len(data):
        if pos + _SAMPLE_LEN_BYTES > len(data):
            return None
        sample_len = int.from_bytes(data[pos : pos + _SAMPLE_LEN_BYTES], "big")
        pos += _SAMPLE_LEN_BYTES
        if pos + sample_len > len(data):
            return None
        samples.append(data[pos : pos + sample_len])
        pos += sample_len
    if not samples or pos != len(data):
        return None
    return device_id, samples


# ---------------------------------------------------------------------------
# API 模型
# ---------------------------------------------------------------------------


class SampleIn(BaseModel):
    """一条遥测样本的受理请求。"""

    model_config = ConfigDict(extra="forbid")

    device_id: str = Field(..., min_length=1, description="设备号（1~32 字节 UTF-8 字符串）")
    sample_hex: str = Field(..., description="十六进制样本原文（偶数位，仅含 0-9a-f/A-F）")

    @field_validator("device_id")
    @classmethod
    def _check_device_bytes(cls, v: str) -> str:
        if len(v.encode("utf-8")) > MAX_DEVICE_ID_BYTES:
            raise ValueError(f"设备号 UTF-8 编码后不得超过 {MAX_DEVICE_ID_BYTES} 字节")
        return v

    @field_validator("sample_hex")
    @classmethod
    def _check_hex(cls, v: str) -> str:
        s = v.strip()
        if len(s) % 2 != 0:
            raise ValueError("十六进制样本位数必须为偶数")
        try:
            bytes.fromhex(s)
        except ValueError as exc:
            raise ValueError("sample_hex 不是合法的十六进制字符串") from exc
        return s


class TelemetryConfigIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    payload_limit: int = Field(
        ...,
        ge=MIN_PAYLOAD_LIMIT,
        le=MAX_PAYLOAD_LIMIT,
        description=f"UDP 负载上限（{MIN_PAYLOAD_LIMIT}~{MAX_PAYLOAD_LIMIT} 字节，含封装开销）",
    )


class SampleAck(BaseModel):
    accepted: bool
    device_id: str
    sample_hex: str
    sample_bytes: int
    enqueued_at: datetime = Field(..., description="受理时刻（也是等待计时起点，UTC）")
    payload_limit: int
    packet_bytes_if_alone: int = Field(..., description="该样本单独成包时的完整负载字节数")


class ReceivedRecord(BaseModel):
    device_id: str
    packet_bytes: int = Field(..., description="本 UDP 数据报的实际负载字节数")
    samples_hex: list[str] = Field(..., description="包内样本原文（按包内顺序）")
    received_at: datetime


class BatcherStats(BaseModel):
    payload_limit: int
    accepted_total: int
    rejected_oversize_total: int
    datagrams_sent_total: int
    samples_sent_total: int
    pending_samples: int
    pending_by_device: dict[str, int]
    pending_oldest_age_ms: Optional[float]


class ReceiverStats(BaseModel):
    datagrams_received_total: int
    samples_received_total: int
    malformed_datagrams_total: int
    records_buffered: int


# ---------------------------------------------------------------------------
# 合包发送器
# ---------------------------------------------------------------------------


class _PendingSample:
    __slots__ = ("sample", "enqueued_at")

    def __init__(self, sample: bytes, enqueued_at: float) -> None:
        self.sample = sample
        # 等待起点：首次受理时刻；重分包时原封不动保留
        self.enqueued_at = enqueued_at


class Batcher:
    """按设备合包的发送器。所有状态仅在事件循环内访问，并以锁串行化。"""

    def __init__(
        self,
        transport: asyncio.DatagramTransport,
        target: tuple[str, int],
        payload_limit: int = DEFAULT_PAYLOAD_LIMIT,
    ) -> None:
        self._transport = transport
        self._target = target
        self._limit = payload_limit
        self._lock = asyncio.Lock()
        # device_id -> 待发样本（受理顺序；等待起点随样本保留）
        self._pending: dict[str, deque[_PendingSample]] = {}
        self._accepted = 0
        self._rejected_oversize = 0
        self._datagrams_sent = 0
        self._samples_sent = 0

    @property
    def payload_limit(self) -> int:
        return self._limit

    # ------------------------------------------------------------------ 受理

    async def submit(self, device_id: str, sample_hex: str) -> SampleAck:
        """受理一条样本；单条成包超限则拒收（状态不变）。"""
        sample = bytes.fromhex(sample_hex)
        alone = single_sample_size(device_id, sample)
        now = time.monotonic()
        async with self._lock:
            if alone > self._limit:
                # 单条样本超限就拒收：不入队、不改变任何待发状态
                self._rejected_oversize += 1
                raise OversizeSample(device_id, len(sample), alone, self._limit)
            queue = self._pending.setdefault(device_id, deque())
            queue.append(_PendingSample(sample, now))
            self._accepted += 1
            # 装下这一条后若包已装满（下一条放不下），立即发包
            self._flush_full_locked(device_id)
            return SampleAck(
                accepted=True,
                device_id=device_id,
                sample_hex=sample_hex,
                sample_bytes=len(sample),
                enqueued_at=datetime.now(timezone.utc),
                payload_limit=self._limit,
                packet_bytes_if_alone=alone,
            )

    # ------------------------------------------------------------ 上限调整

    async def set_payload_limit(self, new_limit: int) -> BatcherStats:
        """调整负载上限并对待发样本重新分包。

        任一待发样本单独成包也超过新上限 → 拒绝调整，旧上限与队列原样保留。
        """
        async with self._lock:
            # 先整体校验：任一待发样本单独成包超限即拒绝，旧上限与队列原样保留
            for device_id, queue in self._pending.items():
                overhead = device_overhead(device_id)
                for item in queue:
                    alone = overhead + _SAMPLE_LEN_BYTES + len(item.sample)
                    if alone > new_limit:
                        raise LimitAdjustmentError(
                            f"设备 {device_id} 存在 {len(item.sample)} 字节的待发样本，"
                            f"单独成包需 {alone} 字节，超过新上限 {new_limit}，拒绝调整"
                        )
            # 校验通过：切换上限并重新分包。受理顺序与等待起点都在
            # _PendingSample 中原样保留；装满的包立即发出，未满的包继续等待
            self._limit = new_limit
            for device_id in list(self._pending.keys()):
                self._flush_full_locked(device_id)
            return self.stats_locked()

    # ------------------------------------------------------------ 定时刷新

    async def flush_due(self) -> None:
        """发送所有“最早样本已等满 2 秒”的设备包。"""
        async with self._lock:
            now = time.monotonic()
            for device_id in list(self._pending.keys()):
                queue = self._pending.get(device_id)
                if not queue:
                    continue
                # 反复取最长前缀：上限调低后可能攒出多个待发包，逐包判定
                while queue:
                    packet = self._longest_prefix_locked(device_id, queue)
                    if packet is None:
                        break
                    earliest = queue[0].enqueued_at
                    if now - earliest >= MAX_AGE_S:
                        self._send_locked(device_id, packet)
                        # _send_locked 已按包内条数弹出队列
                    else:
                        break

    # ------------------------------------------------------------ 状态查询

    def stats_locked(self) -> BatcherStats:
        now = time.monotonic()
        total = 0
        oldest: Optional[float] = None
        by_device: dict[str, int] = {}
        for device_id, queue in self._pending.items():
            n = len(queue)
            if n:
                by_device[device_id] = n
                total += n
                age = now - queue[0].enqueued_at
                if oldest is None or age > oldest:
                    oldest = age
        return BatcherStats(
            payload_limit=self._limit,
            accepted_total=self._accepted,
            rejected_oversize_total=self._rejected_oversize,
            datagrams_sent_total=self._datagrams_sent,
            samples_sent_total=self._samples_sent,
            pending_samples=total,
            pending_by_device=by_device,
            pending_oldest_age_ms=None if oldest is None else round(oldest * 1000.0, 3),
        )

    async def stats(self) -> BatcherStats:
        async with self._lock:
            return self.stats_locked()

    # ------------------------------------------------------------ 内部实现

    def _longest_prefix_locked(
        self, device_id: str, queue: deque[_PendingSample]
    ) -> Optional[list[bytes]]:
        """按受理顺序取能放进一个包的最长完整样本前缀。"""
        overhead = device_overhead(device_id)
        budget = self._limit - overhead
        if budget < _SAMPLE_LEN_BYTES:
            return None
        packet: list[bytes] = []
        used = 0
        for item in queue:
            need = _SAMPLE_LEN_BYTES + len(item.sample)
            if used + need > budget:
                break
            packet.append(item.sample)
            used += need
        return packet or None

    def _send_locked(self, device_id: str, packet: list[bytes]) -> None:
        datagram = build_datagram(device_id, packet)
        self._transport.sendto(datagram, self._target)
        for _ in packet:
            self._pending[device_id].popleft()
        if not self._pending[device_id]:
            del self._pending[device_id]
        self._datagrams_sent += 1
        self._samples_sent += len(packet)

    def _flush_full_locked(self, device_id: str) -> None:
        """把该设备所有已装满的包发出；最后一个未满的包留待计时。"""
        queue = self._pending.get(device_id)
        while queue:
            packet = self._longest_prefix_locked(device_id, queue)
            if packet is None:
                return
            # 包内前缀之外还有样本，说明下一条放不下 → 包满，立即发
            if len(packet) < len(queue):
                self._send_locked(device_id, packet)
            else:
                return


class OversizeSample(Exception):
    """单条样本超过当前负载上限。"""

    def __init__(self, device_id: str, sample_bytes: int, packet_bytes: int, limit: int) -> None:
        self.device_id = device_id
        self.sample_bytes = sample_bytes
        self.packet_bytes = packet_bytes
        self.limit = limit
        super().__init__(
            f"设备 {device_id} 的 {sample_bytes} 字节样本单独成包需 "
            f"{packet_bytes} 字节，超过当前负载上限 {limit}"
        )


# ---------------------------------------------------------------------------
# 本地接收器
# ---------------------------------------------------------------------------


class TelemetryReceiver(asyncio.DatagramProtocol):
    """从真实 UDP 数据报解码合包样本，保留最近若干条接收记录。"""

    def __init__(self) -> None:
        self.transport: Optional[asyncio.DatagramTransport] = None
        self._records: deque[ReceivedRecord] = deque(maxlen=MAX_RECEIVED_RECORDS)
        self._datagrams = 0
        self._samples = 0
        self._malformed = 0

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.transport = transport  # type: ignore[assignment]

    def datagram_received(self, data: bytes, addr) -> None:  # noqa: ANN001
        parsed = parse_datagram(data)
        if parsed is None:
            self._malformed += 1
            return
        device_id, samples = parsed
        self._datagrams += 1
        self._samples += len(samples)
        self._records.append(
            ReceivedRecord(
                device_id=device_id,
                packet_bytes=len(data),
                samples_hex=[s.hex() for s in samples],
                received_at=datetime.now(timezone.utc),
            )
        )

    def error_received(self, exc: Exception) -> None:  # pragma: no cover
        pass

    def list_records(self) -> list[ReceivedRecord]:
        return list(self._records)

    def stats(self) -> ReceiverStats:
        return ReceiverStats(
            datagrams_received_total=self._datagrams,
            samples_received_total=self._samples,
            malformed_datagrams_total=self._malformed,
            records_buffered=len(self._records),
        )


class SenderProtocol(asyncio.DatagramProtocol):
    """发送端协议：仅统计异步发送错误，不接收数据。"""

    def __init__(self) -> None:
        self.send_errors = 0

    def datagram_received(self, data: bytes, addr) -> None:  # noqa: ANN001
        # 发送 socket 理论上不会收到数据；忽略即可
        pass

    def error_received(self, exc: Exception) -> None:
        self.send_errors += 1
