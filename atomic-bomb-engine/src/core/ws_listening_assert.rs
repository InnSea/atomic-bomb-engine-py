use crate::models::ws_assert_task::WsAssertTask;
use crate::models::ws_endpoint::AssertTrigger;
use jsonpath_lib::select;
use serde_json::Value;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

/// 处理一条 WS 断言任务
/// WS 断言比 HTTP 复杂的地方:
/// - HTTP 是请求-响应一对一, 一次断言一次性判定; WS 是按消息触发, 同一个 endpoint 会反复触发
/// - on_match 决定行为: EveryMessage 每条消息都判定; FirstMatch 命中一次后续视为通过;
///   NoMatch 模式下消息不命中就成功 (常用于"不应该收到 xxx" 场景, 这里简化为命中即成功)
async fn process_ws_assert_task(task: WsAssertTask) {
    let json_value: Option<Value> = match serde_json::from_slice(&task.body_bytes) {
        Ok(v) => Some(v),
        Err(e) => {
            if task.verbose {
                eprintln!("WS 断言: JSON 解析失败 {}", e);
            }
            task.ws_stats.err_count.fetch_add(1, Ordering::Relaxed);
            task.assert_errors.increment(
                task.endpoint_name.clone(),
                format!("JSON解析失败: {}", e),
                task.endpoint_url.clone(),
            );
            None
        }
    };

    let Some(json_val) = json_value else { return };

    for opt in &task.assert_options {
        match select(&json_val, &opt.jsonpath) {
            Ok(results) => {
                let hit_eq = results
                    .get(0)
                    .map(|v| **v == opt.reference_object)
                    .unwrap_or(false);
                match opt.on_match {
                    AssertTrigger::EveryMessage => {
                        if !hit_eq {
                            task.ws_stats.err_count.fetch_add(1, Ordering::Relaxed);
                            task.assert_errors.increment(
                                task.endpoint_name.clone(),
                                format!(
                                    "预期: {:?}, 实际: {:?}",
                                    opt.reference_object,
                                    results.get(0)
                                ),
                                task.endpoint_url.clone(),
                            );
                        }
                    }
                    AssertTrigger::FirstMatch => {
                        // 简化: 命中即成功 (上层若需要 latch 状态可在 stats 上加位)
                        if !hit_eq && results.is_empty() {
                            // 未匹配到也不计错, 等下一条
                            continue;
                        }
                        if !hit_eq {
                            task.ws_stats.err_count.fetch_add(1, Ordering::Relaxed);
                            task.assert_errors.increment(
                                task.endpoint_name.clone(),
                                format!(
                                    "FirstMatch 不一致 预期: {:?}, 实际: {:?}",
                                    opt.reference_object,
                                    results.get(0)
                                ),
                                task.endpoint_url.clone(),
                            );
                        }
                    }
                    AssertTrigger::NoMatch => {
                        // 命中视为成功, 不命中也不报错; 该模式仅做记录
                        if hit_eq && task.verbose {
                            println!("WS-{} NoMatch 命中", task.endpoint_name);
                        }
                    }
                }
            }
            Err(e) => {
                task.ws_stats.err_count.fetch_add(1, Ordering::Relaxed);
                task.assert_errors.increment(
                    task.endpoint_name.clone(),
                    format!("JSONPath 错误: {}", e),
                    task.endpoint_url.clone(),
                );
            }
        }
    }
}

pub fn spawn_ws_assert_workers(
    rx: mpsc::Receiver<WsAssertTask>,
    worker_count: usize,
) -> Vec<tokio::task::JoinHandle<()>> {
    let rx_shared = Arc::new(Mutex::new(rx));
    let n = worker_count.max(1);
    (0..n)
        .map(|_| {
            let rx = rx_shared.clone();
            tokio::spawn(async move {
                loop {
                    let task_opt = {
                        let mut g = rx.lock().await;
                        g.recv().await
                    };
                    match task_opt {
                        Some(t) => process_ws_assert_task(t).await,
                        None => break,
                    }
                }
            })
        })
        .collect()
}
