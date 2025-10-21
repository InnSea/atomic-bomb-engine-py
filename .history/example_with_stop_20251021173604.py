#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
atomic-bomb-engine-py stop 方法使用示例

本示例展示了如何使用新增的 stop 方法来立即停止压测
"""

import asyncio
import atomic_bomb_engine


async def example_basic_stop():
    """
    示例 1: 基本的 stop 使用
    在收到一定数量的结果后停止压测
    """
    print("=" * 60)
    print("示例 1: 基本的 stop 使用")
    print("=" * 60)
    
    runner = atomic_bomb_engine.BatchRunner()
    
    # 启动压测 - 设置 60 秒，但我们会提前停止
    runner.run(
        test_duration_secs=60,
        concurrent_requests=50,
        timeout_secs=10,
        api_endpoints=[
            atomic_bomb_engine.endpoint(
                name="httpbin-test",
                url="http://httpbin.org/delay/1",
                method="GET",
                weight=100,
            ),
        ]
    )
    
    print("压测已启动，将在收到 3 个结果后停止...")
    
    count = 0
    for res in runner:
        if res.get("should_wait"):
            await asyncio.sleep(0.1)
        else:
            count += 1
            print(f"\n[结果 #{count}]")
            print(f"  总请求数: {res.get('total_requests', 0)}")
            print(f"  成功率: {res.get('success_rate', 0):.2f}%")
            print(f"  错误数: {res.get('err_count', 0)}")
            print(f"  RPS: {res.get('rps', 0):.2f}")
            
            # 在收到 3 个结果后停止
            if count >= 3:
                print("\n✓ 达到 3 个结果，调用 stop() 停止压测...")
                runner.stop()
                break
    
    print("✓ 压测已停止\n")


async def example_error_threshold_stop():
    """
    示例 2: 根据错误阈值停止
    当错误数达到一定数量时自动停止压测
    """
    print("=" * 60)
    print("示例 2: 根据错误阈值停止")
    print("=" * 60)
    
    runner = atomic_bomb_engine.BatchRunner()
    
    runner.run(
        test_duration_secs=120,
        concurrent_requests=100,
        timeout_secs=5,
        api_endpoints=[
            atomic_bomb_engine.endpoint(
                name="error-test",
                url="http://httpbin.org/status/500",  # 这个接口会返回 500 错误
                method="GET",
                weight=100,
            ),
        ]
    )
    
    max_errors = 5
    print(f"压测已启动，当错误数达到 {max_errors} 时将自动停止...")
    
    for res in runner:
        if res.get("should_wait"):
            await asyncio.sleep(0.1)
        else:
            err_count = res.get('err_count', 0)
            print(f"\n当前错误数: {err_count}")
            
            if err_count >= max_errors:
                print(f"\n✗ 错误数达到阈值 {max_errors}，停止压测！")
                runner.stop()
                break
    
    print("✓ 压测已停止\n")


async def example_time_based_stop():
    """
    示例 3: 基于时间的停止
    运行一定时间后停止压测
    """
    print("=" * 60)
    print("示例 3: 基于时间的停止（10 秒后停止）")
    print("=" * 60)
    
    runner = atomic_bomb_engine.BatchRunner()
    
    runner.run(
        test_duration_secs=300,  # 设置 5 分钟，但我们会在 10 秒后停止
        concurrent_requests=50,
        timeout_secs=10,
        api_endpoints=[
            atomic_bomb_engine.endpoint(
                name="time-test",
                url="http://httpbin.org/get",
                method="GET",
                weight=100,
            ),
        ]
    )
    
    print("压测已启动，将在 10 秒后停止...")
    
    import time
    start_time = time.time()
    stop_after = 10  # 10 秒后停止
    
    for res in runner:
        if res.get("should_wait"):
            await asyncio.sleep(0.1)
        else:
            elapsed = time.time() - start_time
            print(f"\n已运行: {elapsed:.1f} 秒")
            print(f"  总请求数: {res.get('total_requests', 0)}")
            print(f"  RPS: {res.get('rps', 0):.2f}")
            
            if elapsed >= stop_after:
                print(f"\n✓ 已运行 {elapsed:.1f} 秒，停止压测...")
                runner.stop()
                break
    
    print("✓ 压测已停止\n")


async def example_signal_handler():
    """
    示例 4: 使用信号处理器
    按 Ctrl+C 时优雅地停止压测
    """
    import signal
    
    print("=" * 60)
    print("示例 4: 信号处理器（按 Ctrl+C 停止）")
    print("=" * 60)
    
    runner = atomic_bomb_engine.BatchRunner()
    
    # 定义信号处理器
    def signal_handler(signum, frame):
        print("\n\n接收到停止信号 (Ctrl+C)，正在停止压测...")
        runner.stop()
    
    # 注册信号处理器
    signal.signal(signal.SIGINT, signal_handler)
    
    runner.run(
        test_duration_secs=300,
        concurrent_requests=50,
        timeout_secs=10,
        api_endpoints=[
            atomic_bomb_engine.endpoint(
                name="signal-test",
                url="http://httpbin.org/get",
                method="GET",
                weight=100,
            ),
        ]
    )
    
    print("压测已启动，按 Ctrl+C 可以随时停止...")
    print("（本示例会在收到 5 个结果后自动停止）")
    
    count = 0
    for res in runner:
        if res.get("should_wait"):
            await asyncio.sleep(0.1)
        else:
            count += 1
            print(f"\n[结果 #{count}] RPS: {res.get('rps', 0):.2f}")
            
            # 为了演示方便，在收到 5 个结果后自动停止
            if count >= 5:
                print("\n✓ 收到 5 个结果，自动停止...")
                runner.stop()
                break
    
    print("✓ 压测已停止\n")


async def main():
    """
    运行所有示例
    """
    print("\n" + "=" * 60)
    print("atomic-bomb-engine-py stop 方法使用示例")
    print("=" * 60 + "\n")
    
    try:
        # 示例 1: 基本使用
        await example_basic_stop()
        await asyncio.sleep(1)
        
        # 示例 2: 错误阈值停止（可能会有网络错误，注释掉避免失败）
        # await example_error_threshold_stop()
        # await asyncio.sleep(1)
        
        # 示例 3: 基于时间停止
        await example_time_based_stop()
        await asyncio.sleep(1)
        
        # 示例 4: 信号处理器
        await example_signal_handler()
        
    except KeyboardInterrupt:
        print("\n\n用户中断，退出程序...")
    except Exception as e:
        print(f"\n\n发生错误: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "=" * 60)
    print("所有示例运行完成！")
    print("=" * 60 + "\n")


if __name__ == '__main__':
    asyncio.run(main())

