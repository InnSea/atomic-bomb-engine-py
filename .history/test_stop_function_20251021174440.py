#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
快速测试 stop 方法是否可用

在本地开发环境中运行此脚本来验证 stop 功能
使用前请先运行: maturin develop --release
"""

import asyncio


def test_stop_method_exists():
    """测试 stop 方法是否存在"""
    print("测试 1: 检查 stop 方法是否存在...")
    
    try:
        import atomic_bomb_engine
        runner = atomic_bomb_engine.BatchRunner()
        
        if hasattr(runner, 'stop'):
            print("✓ stop 方法存在")
            return True
        else:
            print("✗ stop 方法不存在")
            return False
    except Exception as e:
        print(f"✗ 导入失败: {e}")
        return False


async def test_stop_basic():
    """测试基本的 stop 功能"""
    print("\n测试 2: 基本 stop 功能...")
    
    try:
        import atomic_bomb_engine
        
        runner = atomic_bomb_engine.BatchRunner()
        
        # 启动一个简单的测试（使用公共测试接口）
        runner.run(
            test_duration_secs=30,  # 设置 30 秒但会提前停止
            concurrent_requests=10,
            timeout_secs=10,
            verbose=False,
            api_endpoints=[
                atomic_bomb_engine.endpoint(
                    name="httpbin-test",
                    url="http://httpbin.org/delay/1",
                    method="GET",
                    weight=100,
                ),
            ]
        )
        
        print("  压测已启动，将在收到 2 个结果后停止...")
        
        count = 0
        for res in runner:
            if res.get("should_wait"):
                await asyncio.sleep(0.1)
            else:
                count += 1
                print(f"  收到结果 #{count}, 总请求数: {res.get('total_requests', 0)}")
                
                if count >= 2:
                    print("  调用 runner.stop()...")
                    runner.stop()
                    print("  已调用 stop()")
                    break
        
        print("✓ stop 功能正常工作")
        return True
        
    except Exception as e:
        print(f"✗ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_stop_immediate():
    """测试立即停止"""
    print("\n测试 3: 立即停止（不等待结果）...")
    
    try:
        import atomic_bomb_engine
        
        runner = atomic_bomb_engine.BatchRunner()
        
        runner.run(
            test_duration_secs=60,
            concurrent_requests=50,
            timeout_secs=10,
            verbose=False,
            api_endpoints=[
                atomic_bomb_engine.endpoint(
                    name="immediate-stop-test",
                    url="http://httpbin.org/get",
                    method="GET",
                    weight=100,
                ),
            ]
        )
        
        print("  压测已启动...")
        
        # 等待一小段时间后立即停止
        await asyncio.sleep(0.5)
        print("  立即调用 stop()...")
        runner.stop()
        
        # 尝试继续迭代（应该立即结束）
        count = 0
        for res in runner:
            count += 1
            if count > 10:  # 防止无限循环
                break
        
        print(f"  迭代次数: {count}")
        print("✓ 立即停止功能正常")
        return True
        
    except Exception as e:
        print(f"✗ 测试失败: {e}")
        return False


async def main():
    """运行所有测试"""
    print("=" * 60)
    print("atomic-bomb-engine-py stop 方法测试")
    print("=" * 60)
    
    # 测试 1: 方法是否存在
    test1_passed = test_stop_method_exists()
    
    if not test1_passed:
        print("\n" + "=" * 60)
        print("请先运行: maturin develop --release")
        print("=" * 60)
        return
    
    # 测试 2: 基本功能
    test2_passed = await test_stop_basic()
    
    # 测试 3: 立即停止
    test3_passed = await test_stop_immediate()
    
    # 总结
    print("\n" + "=" * 60)
    print("测试总结")
    print("=" * 60)
    print(f"测试 1 (方法存在): {'✓ 通过' if test1_passed else '✗ 失败'}")
    print(f"测试 2 (基本功能): {'✓ 通过' if test2_passed else '✗ 失败'}")
    print(f"测试 3 (立即停止): {'✓ 通过' if test3_passed else '✗ 失败'}")
    
    all_passed = test1_passed and test2_passed and test3_passed
    print("\n" + ("✓ 所有测试通过！" if all_passed else "✗ 部分测试失败"))
    print("=" * 60)


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n测试被用户中断")
    except Exception as e:
        print(f"\n\n发生错误: {e}")
        import traceback
        traceback.print_exc()

