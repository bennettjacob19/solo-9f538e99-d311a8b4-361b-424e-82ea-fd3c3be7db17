"""端到端测试：遥测合包发送器 + 本地 UDP 接收器。"""
from __future__ import annotations

import socket
import time

import pytest
from fastapi.testclient import TestClient

from app.main import TELEMETRY_PORT, app
from app.telemetry import (
    DEFAULT_PAYLOAD_LIMIT,
    MAX_DEVICE_ID_BYTES,
    build_datagram,
    parse_datagram,
)


@pytest.fixture()
def client():
    # 每个用例一套全新 lifespan：接收器、待发队列、统计全部重置
    with TestClient(app) as c:
        yield c


def wait_until(predicate, timeout=3.0, interval=0.02):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def received(client):
    r = client.get("/telemetry/received")
    assert r.status_code == 200, r.text
    body = r.json()
    return body["records"], body["stats"]


def submit(client, device_id, sample_hex):
    return client.post(
        "/telemetry/samples", json={"device_id": device_id, "sample_hex": sample_hex}
    )


def set_limit(client, limit):
    return client.put("/telemetry/config", json={"payload_limit": limit})


def config(client):
    return client.get("/telemetry/config").json()


# ---------------------------------------------------------------- 线格式


class TestWireFormat:
    def test_roundtrip(self):
        pkt = build_datagram("dev-1", [b"\x00\x01", b"ab", b"\xff" * 10])
        assert parse_datagram(pkt) == ("dev-1", [b"\x00\x01", b"ab", b"\xff" * 10])

    def test_empty_device_or_no_samples_rejected(self):
        with pytest.raises(ValueError):
            build_datagram("dev", [])

    def test_parse_rejects_garbage(self):
        assert parse_datagram(b"") is None
        assert parse_datagram(b"X" * 20) is None  # magic 不符
        # magic 对但 dev_len=0
        assert parse_datagram(b"TEL1\x00") is None
        # 长度字段声称的样本超出数据报
        bad = b"TEL1\x01d" + b"\x00\x10short"
        assert parse_datagram(bad) is None
        # 样本长度字段后缺字节
        bad2 = b"TEL1\x01d" + b"\x00\x05ab"
        assert parse_datagram(bad2) is None

    def test_header_overhead_accounted(self):
        # 设备 "d"：固定头 5 + 设备号 1 = 6；每条样本再加 2 字节长度前缀
        pkt = build_datagram("d", [b"\xaa"])
        assert len(pkt) == 6 + 2 + 1
        assert parse_datagram(pkt) == ("d", [b"\xaa"])


# ---------------------------------------------------------------- 受理与拒收


class TestSubmit:
    def test_accept_returns_ack_and_pending(self, client):
        r = submit(client, "dev-a", "0102")
        assert r.status_code == 200, r.text
        ack = r.json()
        assert ack["accepted"] is True
        assert ack["device_id"] == "dev-a"
        assert ack["sample_hex"] == "0102"
        assert ack["sample_bytes"] == 2
        assert ack["payload_limit"] == DEFAULT_PAYLOAD_LIMIT
        # 单独成包字节数：固定头 5 + 设备号 5 + 长度前缀 2 + 样本 2 = 14
        assert ack["packet_bytes_if_alone"] == 14
        assert "enqueued_at" in ack
        stats = config(client)["stats"]
        assert stats["accepted_total"] == 1
        assert stats["pending_samples"] == 1
        assert stats["pending_by_device"] == {"dev-a": 1}

    def test_invalid_hex_rejected_422(self, client):
        for bad in ("zz", "abc", "12 34", "0g"):
            r = submit(client, "dev-a", bad)
            assert r.status_code == 422, bad
        stats = config(client)["stats"]
        assert stats["accepted_total"] == 0

    def test_invalid_device_rejected_422(self, client):
        assert submit(client, "", "01").status_code == 422
        assert submit(client, "x" * (MAX_DEVICE_ID_BYTES + 1), "01").status_code == 422
        # 多字节 UTF-8：33 个 ASCII 字符已超长；11 个三字节中文 = 33 字节也超长
        assert submit(client, "中" * 11, "01").status_code == 422
        # 额外字段拒绝
        r = client.post(
            "/telemetry/samples",
            json={"device_id": "d", "sample_hex": "01", "extra": 1},
        )
        assert r.status_code == 422

    def test_oversize_single_sample_rejected_and_counted(self, client):
        # 上限 1400：构造一条单独成包必超 1400 的样本
        big = "ab" * 1400  # 1400 字节样本，含封装必然超限
        r = submit(client, "dev-a", big)
        assert r.status_code == 422
        detail = r.json()["detail"]
        assert "超限" in detail
        stats = config(client)["stats"]
        assert stats["rejected_oversize_total"] == 1
        assert stats["accepted_total"] == 0
        assert stats["pending_samples"] == 0

    def test_limit_boundary_sample_accepted(self, client):
        # 设备号 "d" 开销 6，样本长度前缀 2；样本 1392 字节时整包恰好 1400
        boundary = (bytes([0xCD]) * 1392).hex()
        r = submit(client, "d", boundary)
        assert r.status_code == 200, r.text
        assert r.json()["packet_bytes_if_alone"] == 1400


# ---------------------------------------------------------------- 合包行为


class TestBatching:
    def test_pending_until_timeout_then_single_datagram(self, client):
        for hx in ("01", "0203", "040506"):
            assert submit(client, "dev-a", hx).status_code == 200
        # 尚未到 2 秒：接收器无记录，样本仍在待发
        time.sleep(0.2)
        records, stats = received(client)
        assert stats["datagrams_received_total"] == 0
        assert config(client)["stats"]["pending_samples"] == 3

        assert wait_until(lambda: received(client)[1]["datagrams_received_total"] == 1)
        records, stats = received(client)
        assert stats["datagrams_received_total"] == 1
        assert stats["samples_received_total"] == 3
        rec = records[0]
        assert rec["device_id"] == "dev-a"
        assert rec["samples_hex"] == ["01", "0203", "040506"]
        # 每包负载字节数来自实际 UDP 数据报：
        # 固定头 5 + 设备号 5 + (2+1)+(2+2)+(2+3) = 10+5+4+3 = 22
        assert rec["packet_bytes"] == 22
        assert config(client)["stats"]["pending_samples"] == 0

    def test_packet_full_flushes_immediately(self, client):
        # 小样本（1 字节），设备 "d" 每包可容纳 n 条：6 + n*(2+1) <= 1400 → n=464
        # 464 条装入后整包 1398 字节仍未装满；第 465 条放不下，触发当前包立即发送
        for i in range(465):
            submit(client, "d", "aa")
        assert wait_until(lambda: received(client)[1]["datagrams_received_total"] == 1)
        records, _ = received(client)
        assert records[0]["device_id"] == "d"
        assert len(records[0]["samples_hex"]) == 464
        assert records[0]["packet_bytes"] == 1398

        # 第 465 条留在新包里等待，不应立刻发出
        time.sleep(0.15)
        _, stats = received(client)
        assert stats["datagrams_received_total"] == 1
        assert config(client)["stats"]["pending_by_device"] == {"d": 1}

    def test_next_sample_does_not_fit_flushes_current_packet(self, client):
        # 先把上限调到 256，便于小样本快速装满
        assert set_limit(client, 256).status_code == 200
        # 设备 "d"：开销 6；每条 1 字节样本占 3 字节。装入 83 条用掉 249，
        # 第 84 条需要 252 > 250 预算 → 第 84 条到达时前 83 条立即成包
        for _ in range(83):
            submit(client, "d", "11")
        time.sleep(0.1)
        assert received(client)[1]["datagrams_received_total"] == 0
        submit(client, "d", "22")
        assert wait_until(lambda: received(client)[1]["datagrams_received_total"] == 1)
        records, _ = received(client)
        assert len(records[0]["samples_hex"]) == 83
        assert records[0]["packet_bytes"] == 6 + 83 * 3  # 255
        # 第 84 条留在新包里等待
        assert config(client)["stats"]["pending_by_device"] == {"d": 1}

    def test_two_devices_batched_independently(self, client):
        submit(client, "dev-a", "01")
        submit(client, "dev-b", "aa")
        submit(client, "dev-a", "02")
        submit(client, "dev-b", "bb")
        stats = config(client)["stats"]
        assert stats["pending_by_device"] == {"dev-a": 2, "dev-b": 2}

        assert wait_until(lambda: received(client)[1]["datagrams_received_total"] == 2)
        records, rstats = received(client)
        assert rstats["samples_received_total"] == 4
        by_dev = {r["device_id"]: r["samples_hex"] for r in records}
        assert by_dev == {"dev-a": ["01", "02"], "dev-b": ["aa", "bb"]}
        for r in records:
            assert r["packet_bytes"] > 0

    def test_flush_age_uses_earliest_sample(self, client):
        submit(client, "d", "01")
        time.sleep(1.0)
        submit(client, "d", "02")  # 后到样本不重置等待起点
        # 首条受理后约 2 秒即应发包（而不是第二条之后再等 2 秒）
        assert wait_until(
            lambda: received(client)[1]["datagrams_received_total"] == 1, timeout=2.5
        )
        records, _ = received(client)
        assert records[0]["samples_hex"] == ["01", "02"]


class TestLimitAdjustment:
    def test_valid_range_validated(self, client):
        assert set_limit(client, 255).status_code == 422
        assert set_limit(client, 1401).status_code == 422
        assert set_limit(client, "x").status_code == 422
        # 256 与 1400 为合法边界
        assert set_limit(client, 256).status_code == 200
        assert set_limit(client, 1400).status_code == 200
        r = client.put("/telemetry/config", json={"payload_limit": 512, "x": 1})
        assert r.status_code == 422

    def test_lower_limit_repacks_preserving_order_and_age(self, client):
        # 设备 d 开销 6；上限 256 → 样本预算 250；1 字节样本每条 3，每包可装 83 条
        assert set_limit(client, 256).status_code == 200
        for _ in range(100):
            submit(client, "d", "77")
        time.sleep(0.1)
        # 受理过程中第 84 条放不下，前 83 条已立即成包发出；余 17 条待发
        records, _ = received(client)
        assert len(records) == 1 and len(records[0]["samples_hex"]) == 83
        assert config(client)["stats"]["pending_by_device"] == {"d": 17}

        # 以相同上限重放调整：仍返回 repacked，队列保持不变
        r = set_limit(client, 256)
        assert r.status_code == 200
        assert r.json()["repacked"] is True
        assert config(client)["stats"]["pending_by_device"] == {"d": 17}

        # 等待起点保留：首条（已满包那批之外的队首）受理后约 2 秒发包
        assert wait_until(
            lambda: received(client)[1]["datagrams_received_total"] == 2, timeout=2.5
        )
        records, stats = received(client)
        newest = records[0]
        assert newest["device_id"] == "d"
        assert len(newest["samples_hex"]) == 17
        assert newest["packet_bytes"] == 6 + 17 * 3  # 57 <= 256
        assert stats["samples_received_total"] == 100
        # 重分包不改变受理顺序：所有样本原文一致
        all_hex = [s for rec in records for s in rec["samples_hex"]]
        assert all_hex == ["77"] * 100

    def test_lower_limit_flushes_full_new_packets_immediately(self, client):
        # 默认 1400 下受理 100 条 1 字节样本：整包 306 字节，全部待发
        for _ in range(100):
            submit(client, "d", "99")
        assert config(client)["stats"]["pending_samples"] == 100
        # 调到 256：重分包后 83 条装满立即发，余 17 条等待
        assert set_limit(client, 256).status_code == 200
        assert wait_until(lambda: received(client)[1]["datagrams_received_total"] == 1)
        records, _ = received(client)
        assert len(records[0]["samples_hex"]) == 83
        assert records[0]["packet_bytes"] == 255
        assert config(client)["stats"]["pending_by_device"] == {"d": 17}

    def test_reject_adjustment_when_pending_sample_cannot_fit_alone(self, client):
        # 设备 "d" 受理一条 1300 字节样本：单独成包 6+2+1300=1308 <= 1400，受理
        big_hex = (bytes([0xAB]) * 1300).hex()
        assert submit(client, "d", big_hex).status_code == 200
        # 调到 256：1308 > 256，拒绝；旧上限不变、队列不动
        r = set_limit(client, 256)
        assert r.status_code == 409
        body = r.json()["detail"]
        assert body["error"] == "payload_limit_adjustment_rejected"
        assert body["rejected_limit"] == 256
        assert body["current_payload_limit"] == 1400
        assert config(client)["payload_limit"] == 1400
        assert config(client)["stats"]["pending_samples"] == 1

        # 旧上限继续生效：样本等满 2 秒后照常发出
        assert wait_until(lambda: received(client)[1]["datagrams_received_total"] == 1)
        records, stats = received(client)
        assert records[0]["packet_bytes"] == 1308
        assert records[0]["samples_hex"] == [big_hex]
        assert stats["samples_received_total"] == 1

    def test_timer_flush_emits_all_due_packets_after_limit_cut(self, client):
        # 1400 下受理 200 条 1 字节样本（一个包即可装下，全部待发）
        for _ in range(200):
            submit(client, "d", "44")
        assert config(client)["stats"]["pending_samples"] == 200
        # 调低到 256：重分包为 83+83+34，前两个满包立即发，余 34 条等待
        assert set_limit(client, 256).status_code == 200
        assert wait_until(lambda: received(client)[1]["datagrams_received_total"] == 2)
        assert config(client)["stats"]["pending_by_device"] == {"d": 34}
        # 余下 34 条等满 2 秒后由定时刷新一次性发出，等待起点保持不变
        assert wait_until(lambda: received(client)[1]["datagrams_received_total"] == 3)
        records, stats = received(client)
        assert stats["samples_received_total"] == 200
        assert len(records[0]["samples_hex"]) == 34  # 最新记录在前
        assert all(s == "44" for s in records[0]["samples_hex"])

    def test_raise_limit_then_more_samples_fit(self, client):
        assert set_limit(client, 256).status_code == 200
        for _ in range(10):
            submit(client, "d", "33")
        assert config(client)["stats"]["pending_samples"] == 10
        assert set_limit(client, 1400).status_code == 200
        assert config(client)["stats"]["pending_samples"] == 10
        # 调高后同样等待 2 秒一个包发出
        assert wait_until(lambda: received(client)[1]["datagrams_received_total"] == 1)
        records, _ = received(client)
        assert len(records[0]["samples_hex"]) == 10

    def test_two_devices_one_oversize_blocks_only_itself(self, client):
        # 设备 d 放入可在 256 下容纳的样本；设备 big 的大样本阻止调低
        assert set_limit(client, 1400).status_code == 200
        submit(client, "d", "1122")
        big_hex = (bytes([9]) * 1000).hex()
        assert submit(client, "big", big_hex).status_code == 200
        r = set_limit(client, 256)
        assert r.status_code == 409
        # 两台设备的待发队列都原样保留，旧上限继续计时发送
        pending = config(client)["stats"]["pending_by_device"]
        assert pending == {"d": 1, "big": 1}
        assert wait_until(lambda: received(client)[1]["datagrams_received_total"] == 2)
        records, stats = received(client)
        assert stats["samples_received_total"] == 2
        by_dev = {r["device_id"]: r["samples_hex"] for r in records}
        assert by_dev["d"] == ["1122"]
        assert by_dev["big"] == [big_hex]

    def test_rejected_adjustment_does_not_affect_acceptance_counters(self, client):
        big_hex = (bytes([5]) * 1000).hex()
        assert submit(client, "big", big_hex).status_code == 200
        before = config(client)["stats"]
        assert set_limit(client, 256).status_code == 409
        after = config(client)["stats"]
        assert after["accepted_total"] == before["accepted_total"]
        assert after["rejected_oversize_total"] == before["rejected_oversize_total"]
        assert after["pending_samples"] == before["pending_samples"]


# ---------------------------------------------------------------- 接收器


class TestReceiver:
    def test_received_payload_bytes_match_real_datagram(self, client):
        submit(client, "dev-x", "deadbeef")
        assert wait_until(lambda: received(client)[1]["datagrams_received_total"] == 1)
        records, _ = received(client)
        rec = records[0]
        # 固定头 5 + 设备号 5 + 长度前缀 2 + 样本 4 = 16
        assert rec["packet_bytes"] == 16
        assert rec["samples_hex"] == ["deadbeef"]
        assert rec["device_id"] == "dev-x"
        assert "received_at" in rec

    def test_malformed_datagram_counted_not_stored(self, client):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            sock.sendto(b"not-a-telemetry-packet", ("127.0.0.1", TELEMETRY_PORT))
            sock.sendto(b"TEL1\x01d" + b"\x00\x05ab", ("127.0.0.1", TELEMETRY_PORT))
        finally:
            sock.close()
        assert wait_until(
            lambda: received(client)[1]["malformed_datagrams_total"] == 2
        )
        records, stats = received(client)
        assert stats["malformed_datagrams_total"] == 2
        assert stats["datagrams_received_total"] == 0
        assert records == []

    def test_valid_datagram_after_malformed_one_still_decoded(self, client):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            sock.sendto(b"garbage", ("127.0.0.1", TELEMETRY_PORT))
        finally:
            sock.close()
        submit(client, "d", "01")
        assert wait_until(lambda: received(client)[1]["datagrams_received_total"] == 1)
        records, stats = received(client)
        assert stats["malformed_datagrams_total"] == 1
        assert records[0]["samples_hex"] == ["01"]


# ---------------------------------------------------------------- 兼容性


class TestCompatibility:
    def test_original_apis_still_work(self, client):
        assert client.get("/echo/config").status_code == 200
        r = client.post("/probe", json={"timeout_ms": 150})
        assert r.status_code == 200
        assert r.json()["status"] == "ok"
        assert client.get("/probes").status_code == 200

    def test_docs_list_telemetry_endpoints(self, client):
        assert client.get("/docs").status_code == 200
        spec = client.get("/openapi.json").json()
        for path in (
            "/telemetry/samples",
            "/telemetry/config",
            "/telemetry/received",
            "/echo/config",
            "/probe",
        ):
            assert path in spec["paths"]
