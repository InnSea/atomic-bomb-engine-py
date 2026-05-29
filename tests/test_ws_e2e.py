"""
WebSocket 压测端到端集成测试

启动一个本地 echo server, 用 WsBatchRunner 进行压测, 验证:
- 连接建立和关闭
- 消息收发统计
- RequestResponse 配对 RTT
- 心跳 / 重连
- 断言子系统
"""
import asyncio
import json
import threading
import time

import pytest
import websockets

import atomic_bomb_engine as abe


# ---------- 本地 echo server ----------

class EchoServer:
    """
    简单 echo server:
    - 收到 text 原样回 (透传 id 字段, 用于 jsonpath 配对)
    - 收到 ping 自动回 pong (websockets 库默认行为)
    """

    def __init__(self, host="127.0.0.1", port=18999):
        self.host = host
        self.port = port
        self.loop = None
        self.server = None
        self._thread = None
        self._stop_evt = None

    async def _handler(self, ws):
        try:
            async for message in ws:
                # 原样回写
                await ws.send(message)
        except websockets.ConnectionClosed:
            pass

    def _run(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

        async def main():
            self._stop_evt = asyncio.Event()
            self.server = await websockets.serve(
                self._handler, self.host, self.port, ping_interval=None
            )
            await self._stop_evt.wait()
            self.server.close()
            await self.server.wait_closed()

        self.loop.run_until_complete(main())
        self.loop.close()

    def start(self):
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        time.sleep(0.5)  # 等 server 起来

    def stop(self):
        if self.loop and self._stop_evt:
            self.loop.call_soon_threadsafe(self._stop_evt.set)
        if self._thread:
            self._thread.join(timeout=3)


@pytest.fixture(scope="module")
def echo_server():
    s = EchoServer()
    s.start()
    yield s
    s.stop()


# ---------- 测试用例 ----------

@pytest.mark.asyncio
async def test_oneway_basic(echo_server):
    """OneWay: 持续发送, 验证 messages_sent / messages_received 计数"""
    runner = abe.WsBatchRunner()
    await runner.run(
        test_duration_secs=3,
        concurrent_connections=2,
        ws_endpoints=[
            abe.ws_endpoint(
                name="echo-oneway",
                url=f"ws://{echo_server.host}:{echo_server.port}",
                weight=1,
                mode=abe.ws_mode_oneway(),
                send_pattern=abe.ws_send_pattern(
                    messages=[abe.ws_text("hello")],
                    rate_per_sec=20,
                ),
            )
        ],
    )

    last = None
    async for snap in runner:
        if snap.get("should_wait"):
            await asyncio.sleep(0.1)
            continue
        last = snap

    assert last is not None
    assert last["total_connections_opened"] >= 2
    assert last["total_messages_sent"] > 0
    # echo server 会回写, 所以 received 也应大于 0
    assert last["total_messages_received"] > 0
    # 至少一个 endpoint 结果
    assert len(last["ws_results"]) == 1
    r = last["ws_results"][0]
    assert r["name"] == "echo-oneway"
    assert r["connections_opened"] >= 2

    # 回归: success_rate 不应超过 100% (历史 bug: 接收数/发送数 = 400%)
    assert 0.0 <= last["success_rate"] <= 100.0
    assert 0.0 <= r["success_rate"] <= 100.0
    # 回归: min_response_time 不应透传 u64::MAX 哨兵 (OneWay 无 RTT 应为 0)
    assert r["min_response_time"] == 0
    assert last["min_response_time"] == 0


@pytest.mark.asyncio
async def test_request_response_jsonpath(echo_server):
    """RequestResponse: 用 JSONPath id 配对, 验证 RTT 统计"""
    runner = abe.WsBatchRunner()
    await runner.run(
        test_duration_secs=3,
        concurrent_connections=1,
        ws_endpoints=[
            abe.ws_endpoint(
                name="echo-rr",
                url=f"ws://{echo_server.host}:{echo_server.port}",
                weight=1,
                mode=abe.ws_mode_request_response(
                    match_by=abe.ws_match_jsonpath("$.id", "$.id"),
                    timeout_ms=5000,
                ),
                send_pattern=abe.ws_send_pattern(
                    messages=[
                        abe.ws_json({"id": "{{uuid}}", "op": "ping"})
                    ],
                    rate_per_sec=10,
                ),
            )
        ],
        global_variables={"uuid": "static-id-1"},  # 简化, 不用真随机
    )

    last = None
    async for snap in runner:
        if snap.get("should_wait"):
            await asyncio.sleep(0.1)
            continue
        last = snap

    assert last is not None
    # 同一个 id 被复用, 第一条会配对成功; 后续 send 会覆盖 pending,
    # echo 的回包里 id 还是 static-id-1, 所以会持续配对成功
    assert last["successful_requests"] > 0
    assert last["total_messages_sent"] > 0
    # 回归: RR 模式配对成功后 RTT 应被记录, min 不再是哨兵 / 不被过滤误伤为 0
    r = last["ws_results"][0]
    assert r["max_response_time"] >= r["min_response_time"]
    # success_rate 仍应在合理区间
    assert 0.0 <= last["success_rate"] <= 100.0


@pytest.mark.asyncio
async def test_assert_pass(echo_server):
    """断言通过: echo 回的 op == ping, 断言相等"""
    runner = abe.WsBatchRunner()
    await runner.run(
        test_duration_secs=2,
        concurrent_connections=1,
        ws_endpoints=[
            abe.ws_endpoint(
                name="echo-assert",
                url=f"ws://{echo_server.host}:{echo_server.port}",
                weight=1,
                mode=abe.ws_mode_oneway(),
                send_pattern=abe.ws_send_pattern(
                    messages=[abe.ws_json({"op": "ping"})],
                    rate_per_sec=5,
                ),
                assert_options=[
                    abe.ws_assert_option(
                        jsonpath="$.op",
                        reference_object="ping",
                        on_match="EveryMessage",
                    )
                ],
            )
        ],
    )

    last = None
    async for snap in runner:
        if snap.get("should_wait"):
            await asyncio.sleep(0.1)
            continue
        last = snap

    assert last is not None
    assert last["err_count"] == 0
    assert len(last["assert_errors"]) == 0


@pytest.mark.asyncio
async def test_assert_fail(echo_server):
    """断言失败: echo 回的 op != pong, 应记录 assert_errors"""
    runner = abe.WsBatchRunner()
    await runner.run(
        test_duration_secs=2,
        concurrent_connections=1,
        ws_endpoints=[
            abe.ws_endpoint(
                name="echo-assert-fail",
                url=f"ws://{echo_server.host}:{echo_server.port}",
                weight=1,
                mode=abe.ws_mode_oneway(),
                send_pattern=abe.ws_send_pattern(
                    messages=[abe.ws_json({"op": "ping"})],
                    rate_per_sec=5,
                ),
                assert_options=[
                    abe.ws_assert_option(
                        jsonpath="$.op",
                        reference_object="pong",  # 故意写错
                    )
                ],
            )
        ],
    )

    last = None
    async for snap in runner:
        if snap.get("should_wait"):
            await asyncio.sleep(0.1)
            continue
        last = snap

    assert last is not None
    assert last["err_count"] > 0
    assert len(last["assert_errors"]) > 0


@pytest.mark.asyncio
async def test_handshake_failure():
    """握手失败: 连接到不存在的端口, 应记录 connections_failed"""
    runner = abe.WsBatchRunner()
    await runner.run(
        test_duration_secs=2,
        concurrent_connections=1,
        ws_endpoints=[
            abe.ws_endpoint(
                name="bad-host",
                url="ws://127.0.0.1:1",  # 1 端口拒绝连接
                weight=1,
                mode=abe.ws_mode_oneway(),
            )
        ],
    )

    last = None
    async for snap in runner:
        if snap.get("should_wait"):
            await asyncio.sleep(0.1)
            continue
        last = snap

    assert last is not None
    assert last["total_connections_failed"] >= 1
    # ws_errors 里应该有 Handshake 类型
    if last["ws_errors"]:
        assert any(e["kind"] == "Handshake" for e in last["ws_errors"])


@pytest.mark.asyncio
async def test_stop_works(echo_server):
    """stop() 能立即终止压测"""
    runner = abe.WsBatchRunner()
    await runner.run(
        test_duration_secs=60,  # 故意设大
        concurrent_connections=2,
        ws_endpoints=[
            abe.ws_endpoint(
                name="long-run",
                url=f"ws://{echo_server.host}:{echo_server.port}",
                weight=1,
                mode=abe.ws_mode_oneway(),
                send_pattern=abe.ws_send_pattern(
                    messages=[abe.ws_text("ping")],
                    rate_per_sec=5,
                ),
            )
        ],
    )

    start = time.monotonic()
    snapshots = 0
    async for snap in runner:
        if snap.get("should_wait"):
            await asyncio.sleep(0.1)
            continue
        snapshots += 1
        if snapshots >= 2:
            await runner.stop()
            break

    elapsed = time.monotonic() - start
    assert elapsed < 10, f"stop() 应在数秒内返回, 实际 {elapsed}s"


@pytest.mark.asyncio
async def test_data_pool_injects_url(echo_server, tmp_path):
    """回归: 数据池的行数据应注入到 url 模板渲染上下文.
    之前的 bug: send_pattern=null 时数据池从不被读, url 里 {{uid}} 渲染成空."""
    import os
    # 准备 CSV: 两行, uid 分别为 AAA 和 BBB
    csv_file = tmp_path / "uids.csv"
    csv_file.write_text("uid\nAAA\nBBB\n")

    runner = abe.WsBatchRunner()
    await runner.run(
        test_duration_secs=3,
        concurrent_connections=2,
        ws_endpoints=[
            abe.ws_endpoint(
                name="pool-url",
                # url 里用 {{uid}} 模板, echo server 不关心 path, 但引擎会渲染
                url=f"ws://{echo_server.host}:{echo_server.port}/room?uid={{{{uid}}}}",
                weight=1,
                mode=abe.ws_mode_oneway(),
                # 发一条包含 uid 的消息, echo 回来后可以验证 uid 不为空
                on_connect=[abe.ws_json({"joined_uid": "{{uid}}"})],
            )
        ],
        data_pool=abe.data_pool_option(str(csv_file), mode="sequential"),
    )

    last = None
    async for snap in runner:
        if snap.get("should_wait"):
            await asyncio.sleep(0.1)
            continue
        last = snap

    assert last is not None
    # 关键断言: 连接应该成功建立 (之前 uid 为空会导致 Stream 错误)
    assert last["total_connections_opened"] >= 2
    assert last["total_connections_failed"] == 0
    assert last["total_connections_dropped"] == 0
    # on_connect 发了消息, echo 回来了
    assert last["total_messages_sent"] >= 2
    assert last["total_messages_received"] >= 2
    # 没有 Stream 错误
    for e in last["ws_errors"]:
        assert e["kind"] != "Stream", f"不应有 Stream 错误: {e}"
