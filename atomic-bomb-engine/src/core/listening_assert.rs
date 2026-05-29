use crate::models::assert_task::AssertTask;
use jsonpath_lib::select;
use serde_json::Value;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

fn normalize_serde_error(e: &serde_json::Error) -> String {
    let msg = format!("{}", e);
    if let Some(pos) = msg.find(" at line ") {
        format!("JSONPath查询失败:{}", &msg[..pos])
    } else {
        format!("JSONPath查询失败:{}", msg)
    }
}

/// 处理一条断言任务
/// 成功 / 失败原子计数由本函数自己写入, 调用方不再需要等 oneshot
async fn process_assert_task(task: AssertTask) {
    let mut assertion_failed = false;
    let json_value: Option<Value> = match serde_json::from_slice(&*task.body_bytes) {
        Err(e) => {
            if task.verbose {
                eprintln!("JSONPath 查询失败: {}", e);
            }
            task.err_count.fetch_add(1, Ordering::Relaxed);
            task.api_err_count.fetch_add(1, Ordering::Relaxed);
            assertion_failed = true;
            task.assert_errors.increment(
                task.api_name.clone(),
                normalize_serde_error(&e),
                task.endpoint.url.clone(),
            );
            None
        }
        Ok(val) => Some(val),
    };
    for assert_option in &task.assert_options {
        if task.body_bytes.len() == 0 {
            break;
        }
        if let Some(json_val) = json_value.clone() {
            match select(&json_val, &*assert_option.jsonpath) {
                Ok(results) => {
                    if results.is_empty() {
                        if task.verbose {
                            eprintln!("没有匹配到任何结果");
                        }
                        task.err_count.fetch_add(1, Ordering::Relaxed);
                        task.api_err_count.fetch_add(1, Ordering::Relaxed);
                        task.assert_errors.increment(
                            task.api_name.clone(),
                            "没有匹配到任何结果".to_string(),
                            task.endpoint.url.clone(),
                        );
                        assertion_failed = true;
                        break;
                    }
                    if results.len() > 1 {
                        if task.verbose {
                            eprintln!("匹配到多个值，无法进行断言");
                        }
                        task.err_count.fetch_add(1, Ordering::Relaxed);
                        task.api_err_count.fetch_add(1, Ordering::Relaxed);
                        task.assert_errors.increment(
                            task.api_name.clone(),
                            "匹配到多个值，无法断言".to_string(),
                            task.endpoint.url.clone(),
                        );
                        assertion_failed = true;
                        break;
                    }
                    if let Some(result) = results.get(0).map(|&v| v) {
                        if *result != assert_option.reference_object {
                            task.assert_errors.increment(
                                task.api_name.clone(),
                                format!(
                                    "预期结果：{:?}, 实际结果：{:?}",
                                    assert_option.reference_object, result
                                ),
                                task.endpoint.url.clone(),
                            );
                            if task.verbose {
                                eprintln!(
                                    "{:?}-预期结果：{:?}, 实际结果：{:?}",
                                    task.api_name, assert_option.reference_object, result
                                )
                            }
                            task.err_count.fetch_add(1, Ordering::Relaxed);
                            task.api_err_count.fetch_add(1, Ordering::Relaxed);
                            assertion_failed = true;
                            break;
                        }
                    }
                }
                Err(_) => {
                    assertion_failed = true;
                    break;
                }
            }
        };
    }
    if !assertion_failed {
        task.successful_requests.fetch_add(1, Ordering::Relaxed);
        task.api_successful_requests.fetch_add(1, Ordering::Relaxed);
    }
}

/// 启动 N 个并发 worker, 从共享 Receiver 拉取断言任务处理
/// 返回所有 worker 的 JoinHandle, 便于 batch 在 drain 时 await 确保统计收尾
pub fn spawn_assert_workers(
    rx_assert: mpsc::Receiver<AssertTask>,
    worker_count: usize,
) -> Vec<tokio::task::JoinHandle<()>> {
    let rx_shared = Arc::new(Mutex::new(rx_assert));
    let effective = worker_count.max(1);
    (0..effective)
        .map(|_| {
            let rx = rx_shared.clone();
            tokio::spawn(async move {
                loop {
                    let task_opt = {
                        let mut guard = rx.lock().await;
                        guard.recv().await
                    };
                    match task_opt {
                        Some(task) => {
                            process_assert_task(task).await;
                        }
                        None => break,
                    }
                }
            })
        })
        .collect()
}
