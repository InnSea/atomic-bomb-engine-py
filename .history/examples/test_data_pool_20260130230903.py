"""
数据池功能测试脚本

使用方式:
1. 先启动一个测试服务器（可以用下面的mock服务器）
2. 运行此脚本: python test_data_pool.py

注意: 需要先编译安装 atomic_bomb_engine:
    cd /path/to/atomic-bomb-engine-py
    pip install maturin
    maturin develop
"""

import asyncio
import os
import atomic_bomb_engine as abe


async def test_data_pool_sequential():
    """测试数据池 - 顺序循环模式"""
    print("=" * 50)
    print("测试数据池 - 顺序循环模式 (sequential)")
    print("=" * 50)
    
    # 获取CSV文件路径（与脚本同目录）
    script_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(script_dir, "test_users.csv")
    
    runner = abe.BatchRunner()
    
    await runner.run(
        test_duration_secs=10,          # 测试10秒
        concurrent_requests=5,          # 5个并发
        api_endpoints=[
            abe.endpoint(
                name="登录接口",
                url="http://httpbin.org/post",  # 使用httpbin测试
                method="POST",
                weight=1,
                json={
                    "username": "{{username}}",
                    "password": "{{password}}",
                    "user_id": "{{user_id}}"
                }
            )
        ],
        verbose=False,
        data_pool=abe.data_pool_option(
            file_path=csv_path,
            mode="sequential"  # 顺序循环读取
        )
    )
    
    # 迭代获取结果
    for result in runner:
        if result is None:
            break
        if result.get("should_wait"):
            await asyncio.sleep(0.1)
            continue
        
        print(f"[{result.get('total_duration', 0):.1f}s] "
              f"请求数: {result.get('total_requests', 0)}, "
              f"RPS: {result.get('rps', 0):.1f}, "
              f"成功率: {result.get('success_rate', 0):.1f}%")
        
        # 显示数据池统计
        if result.get('data_pool_stats'):
            dp_stats = result['data_pool_stats']
            print(result)
    
    print("测试完成!\n")


async def test_data_pool_random():
    """测试数据池 - 随机模式"""
    print("=" * 50)
    print("测试数据池 - 随机模式 (random)")
    print("=" * 50)
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(script_dir, "test_users.csv")
    
    runner = abe.BatchRunner()
    
    await runner.run(
        test_duration_secs=10,
        concurrent_requests=5,
        api_endpoints=[
            abe.endpoint(
                name="用户查询",
                url="http://httpbin.org/get",
                method="GET",
                weight=1,
                headers={
                    "X-User-Id": "{{user_id}}",
                    "X-Username": "{{username}}"
                }
            )
        ],
        verbose=False,
        data_pool=abe.data_pool_option(
            file_path=csv_path,
            mode="random"  # 随机读取
        )
    )
    
    for result in runner:
        if result is None:
            break
        if result.get("should_wait"):
            await asyncio.sleep(0.1)
            continue
        
        print(result)
        
        if result.get('data_pool_stats'):
            dp_stats = result['data_pool_stats']
            print(f"  数据池: 总行数={dp_stats['total_rows']}, 模式={dp_stats['mode']}")
    
    print("测试完成!\n")


async def test_data_pool_with_form():
    """测试数据池 - 与表单数据结合"""
    print("=" * 50)
    print("测试数据池 - 表单数据")
    print("=" * 50)
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(script_dir, "test_users.csv")
    
    runner = abe.BatchRunner()
    
    await runner.run(
        test_duration_secs=10,
        concurrent_requests=3,
        api_endpoints=[
            abe.endpoint(
                name="表单提交",
                url="http://httpbin.org/post",
                method="POST",
                weight=1,
                form_data={
                    "username": "{{username}}",
                    "password": "{{password}}",
                    "age": "{{age}}"
                }
            )
        ],
        verbose=False,
        data_pool=abe.data_pool_option(
            file_path=csv_path,
            mode="sequential"
        )
    )
    
    for result in runner:
        if result is None:
            break
        if result.get("should_wait"):
            await asyncio.sleep(0.1)
            continue
        
        print(result)
    
    print("测试完成!\n")


async def main():
    """主函数"""
    print("\n" + "=" * 60)
    print("  Atomic Bomb Engine - 数据池功能测试")
    print("=" * 60 + "\n")
    
    # 检查CSV文件是否存在
    script_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(script_dir, "test_users.csv")
    
    if not os.path.exists(csv_path):
        print(f"错误: 找不到CSV文件: {csv_path}")
        print("请确保 test_users.csv 文件存在于脚本同目录下")
        return
    
    print(f"CSV文件路径: {csv_path}\n")
    
    # 运行测试
    # await test_data_pool_sequential()
    await test_data_pool_random()
    await test_data_pool_with_form()
    
    print("\n所有测试完成!")


if __name__ == "__main__":
    asyncio.run(main())
