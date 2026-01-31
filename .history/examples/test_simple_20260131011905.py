import asyncio
import atomic_bomb_engine as abe


async def test_simple():
    print("开始简单测试...")
    
    runner = abe.BatchRunner()
    
    # 不使用 await
    runner.run(
        test_duration_secs=5,
        concurrent_requests=2,
        api_endpoints=[
            abe.endpoint(
                name="简单测试",
                url="http://httpbin.org/get",
                method="GET",
                weight=1,
            )
        ],
        # 不传 data_pool
    )
    
    count = 0
    for result in runner:
        count += 1
        print(f"[{count}] 收到结果: {type(result)}")
        
        if result is None:
            print("结果为 None，退出")
            break
            
        if result.get("should_wait"):
            print("  -> should_wait, 等待中...")
            await asyncio.sleep(0.1)
            if count > 100:  # 防止无限循环
                print("等待次数过多，可能有问题！")
                break
            continue
        
        print(f"  -> 实际数据: total_requests={result.get('total_requests')}, "
              f"rps={result.get('rps')}")
    
    print(f"循环结束，共迭代 {count} 次")


if __name__ == "__main__":
    asyncio.run(test_simple())
