use crate::core::concurrency_controller::ConcurrencyController;
use crate::core::fixed_size_queue;
use crate::core::setup;
use crate::core::share_ws_result;
use crate::core::sleep_guard::SleepGuard;
use crate::core::start_ws_task;
use crate::core::ws_endpoint_prebuilt::EndpointPrebuilt;
use crate::core::ws_listening_assert;
use crate::models::assert_error_stats::AssertErrorStats;
use crate::models::data_pool::DataPool;
use crate::models::setup::SetupApiEndpoint;
use crate::models::step_option::{InnerStepOption, StepOption};
use crate::models::ws_endpoint::WsEndpoint;
use crate::models::ws_endpoint_stats::WsEndpointStats;
use crate::models::ws_error_stats::WsErrorStats;
use crate::models::ws_result::WsBatchResult;
use anyhow::Error;
use futures::future::join_all;
use handlebars::Handlebars;
use histogram::AtomicHistogram;
use reqwest::header::{HeaderMap, HeaderValue, USER_AGENT};
use reqwest::Client;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use parking_lot::Mutex as PlMutex;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task::JoinHandle;

const ASSERT_WORKER_COUNT: usize = 4;

/// WS 压测主调度器, 与 HTTP 的 batch 完全独立
///
/// 复用的部分: ConcurrencyController, SleepGuard, fixed_size_queue, AssertErrorStats,
/// 模板渲染流程, setup/teardown HTTP 调用机制
pub async fn ws_batch(
    sender: mpsc::Sender<Option<WsBatchResult>>,
    test_duration_secs: u64,
    concurrent_connections: usize,
    timeout_secs: u64,
    cookie_store_enable: bool,
    verbose: bool,
    should_prevent: bool,
    ws_endpoints: Vec<WsEndpoint>,
    step_option: Option<StepOption>,
    setup_options: Option<Vec<SetupApiEndpoint>>,
    mut assert_channel_buffer_size: usize,
    should_stop: Option<Arc<AtomicBool>>,
    data_pool: Option<Arc<DataPool>>,
    global_variables: Option<BTreeMap<String, Value>>,
    teardown_options: Option<Vec<SetupApiEndpoint>>,
) -> anyhow::Result<WsBatchResult> {
    let _guard = SleepGuard::new(should_prevent);

    if ws_endpoints.is_empty() {
        return Err(Error::msg("ws_endpoints 不能为空"));
    }
    if let Some(s) = &step_option {
        let total_steps = test_duration_secs / s.increase_interval.max(1);
        let total_increase = s.increase_step as u64 * total_steps * (total_steps + 1) / 2;
        if total_increase < concurrent_connections as u64 {
            return Err(Error::msg(
                "WS 阶梯加压总并发数在设置的时间内无法增加到目标连接数",
            ));
        }
    }

    if assert_channel_buffer_size == 0 {
        assert_channel_buffer_size = 1024;
    }

    // 直方图按 endpoint 维度独立 (而非全局共享): 全局共享时 3000+ 连接会反复
    // 撞同一个 AtomicHistogram 的 bucket 计数器, 跨核 cache-line 抖动严重.
    // 每个 endpoint 独立直方图后, 同 endpoint 内连接仍共享 (无法避免, 但量级
    // 可控), endpoint 间互不打扰. 聚合时用 Histogram::wrapping_add merge 后取
    // 百分位, 数学上等价.
    let ws_errors = Arc::new(WsErrorStats::new());
    let assert_errors = AssertErrorStats::new();
    let engine_errors: Arc<PlMutex<Vec<String>>> = Arc::new(PlMutex::new(Vec::new()));
    let should_stop_flag = should_stop.unwrap_or_else(|| Arc::new(AtomicBool::new(false)));

    // 全局变量 + setup 阶段的 HTTP 调用复用 HTTP 路径
    let info = os_info::get();
    let user_agent_value = format!(
        "{} {} ({}; {})",
        env!("CARGO_PKG_NAME"),
        env!("CARGO_PKG_VERSION"),
        info.os_type(),
        info.version()
    )
    .parse::<HeaderValue>()
    .map_err(|e| Error::msg(format!("解析 user agent 失败: {:?}", e)))?;

    let mut headers = HeaderMap::new();
    headers.insert(USER_AGENT, user_agent_value);
    let builder = Client::builder()
        .cookie_store(cookie_store_enable)
        .default_headers(headers);
    let http_client = if timeout_secs > 0 {
        builder
            .timeout(Duration::from_secs(timeout_secs))
            .build()
            .map_err(|e| Error::msg(format!("构建 HTTP 客户端失败: {:?}", e)))?
    } else {
        builder
            .build()
            .map_err(|e| Error::msg(format!("构建 HTTP 客户端失败: {:?}", e)))?
    };

    let mut extract_map: BTreeMap<String, Value> = BTreeMap::new();
    let mut is_need_render_template = false;
    if let Some(vars) = global_variables {
        extract_map.extend(vars);
        is_need_render_template = true;
    }
    if data_pool.is_some() {
        is_need_render_template = true;
    }

    let handlebars = Handlebars::new();
    // 全局 setup
    let setup_options_rendered = setup_options
        .map(|opts| {
            opts.into_iter()
                .map(|mut item| {
                    let rendered = handlebars
                        .render_template(&item.url, &json!(extract_map))
                        .unwrap_or_else(|_| item.url.clone());
                    item.url = rendered;
                    item
                })
                .collect::<Vec<_>>()
        });
    let teardown_options_rendered = teardown_options
        .map(|opts| {
            opts.into_iter()
                .map(|mut item| {
                    let rendered = handlebars
                        .render_template(&item.url, &json!(extract_map))
                        .unwrap_or_else(|_| item.url.clone());
                    item.url = rendered;
                    item
                })
                .collect::<Vec<_>>()
        });

    if let Some(setup_opts) = setup_options_rendered {
        is_need_render_template = true;
        match setup::start_setup(setup_opts, extract_map.clone(), http_client.clone()).await {
            Ok(Some(extract)) => {
                extract_map.extend(extract);
            }
            Ok(None) => {}
            Err(e) => return Err(Error::msg(format!("WS 全局 setup 失败: {:?}", e))),
        }
    }

    let extract_map_arc = Arc::new(extract_map);

    // 队列长度: 与 HTTP 的算法保持一致
    let queue_cap = match step_option.clone() {
        None => 10usize,
        Some(s) => {
            let to_max = concurrent_connections / s.increase_step.max(1);
            let time_to_max = to_max as u64 * s.increase_interval.max(1);
            test_duration_secs.saturating_sub(time_to_max) as usize
        }
    };
    let rps_queue = Arc::new(Mutex::new(fixed_size_queue::FixedSizeQueue::new(queue_cap)));
    let api_rps_queue: Arc<Mutex<BTreeMap<String, fixed_size_queue::FixedSizeQueue<f64>>>> =
        Arc::new(Mutex::new(BTreeMap::new()));

    // 断言通道
    let has_assert = ws_endpoints
        .iter()
        .any(|e| e.assert_options.as_ref().map_or(false, |a| !a.is_empty()));
    let (tx_assert_opt, assert_workers) = if has_assert {
        let (tx, rx) = mpsc::channel(assert_channel_buffer_size);
        let workers = ws_listening_assert::spawn_ws_assert_workers(rx, ASSERT_WORKER_COUNT);
        (Some(tx), workers)
    } else {
        (None, Vec::new())
    };

    let total_weight: u32 = ws_endpoints.iter().map(|e| e.weight).sum::<u32>().max(1);
    let test_start = Instant::now();
    let test_end = test_start + Duration::from_secs(test_duration_secs);
    let last_dura = Arc::new(Mutex::new(0f64));
    let last_total_sent = Arc::new(AtomicUsize::new(0));
    let last_err = Arc::new(AtomicUsize::new(0));

    let mut ws_stats_list: Vec<Arc<WsEndpointStats>> = Vec::new();
    let mut handles: Vec<JoinHandle<Result<(), Error>>> = Vec::new();

    for endpoint in ws_endpoints.into_iter() {
        let weight = endpoint.weight as f64;
        let weight_ratio = weight / total_weight as f64;
        let mut conns_for_endpoint = ((concurrent_connections as f64) * weight_ratio).round() as usize;
        if conns_for_endpoint == 0 {
            conns_for_endpoint = 1;
        }

        // 每个 endpoint 一对独立直方图, 减少跨 endpoint 的 cache-line 争用
        let handshake_hist = Arc::new(
            AtomicHistogram::new(14, 20)
                .map_err(|e| Error::msg(format!("握手直方图初始化失败: {:?}", e)))?,
        );
        let rtt_hist = Arc::new(
            AtomicHistogram::new(14, 20)
                .map_err(|e| Error::msg(format!("RTT 直方图初始化失败: {:?}", e)))?,
        );

        let stats = Arc::new(WsEndpointStats::new(
            endpoint.name.clone(),
            endpoint.url.clone(),
            handshake_hist,
            rtt_hist,
        ));
        ws_stats_list.push(stats.clone());

        let controller = match step_option.clone() {
            None => Arc::new(ConcurrencyController::new(conns_for_endpoint, None)),
            Some(s) => {
                let step = s.increase_step as f64 * weight_ratio;
                Arc::new(ConcurrencyController::new(
                    conns_for_endpoint,
                    Some(InnerStepOption {
                        increase_step: step,
                        increase_interval: s.increase_interval,
                    }),
                ))
            }
        };
        tokio::spawn({
            let c = controller.clone();
            async move {
                c.distribute_permits().await;
            }
        });

        let endpoint_arc = Arc::new(endpoint);
        // 一次性预编译: handlebars 模板注册 + jsonpath 编译, hot path 直接复用.
        // 失败 (例如 jsonpath 语法错) 视为配置错误, 整个 batch 立即返回.
        let prebuilt = Arc::new(
            EndpointPrebuilt::build(&endpoint_arc)
                .map_err(|e| Error::msg(format!("WS endpoint-{} 预编译失败: {:?}", endpoint_arc.name, e)))?,
        );
        for _ in 0..conns_for_endpoint {
            let handle = tokio::spawn(start_ws_task::start_ws_concurrency(
                controller.clone(),
                endpoint_arc.clone(),
                prebuilt.clone(),
                stats.clone(),
                extract_map_arc.clone(),
                test_end,
                should_stop_flag.clone(),
                data_pool.clone(),
                tx_assert_opt.clone(),
                ws_errors.clone(),
                assert_errors.clone(),
                engine_errors.clone(),
                http_client.clone(),
                is_need_render_template,
                verbose,
            ));
            handles.push(handle);
        }
    }
    drop(tx_assert_opt);

    // 启动聚合器
    let (stop_tx, stop_rx) = oneshot::channel();
    tokio::spawn(share_ws_result::collect_ws_results(
        sender.clone(),
        stop_rx,
        ws_stats_list.clone(),
        ws_errors.clone(),
        assert_errors.clone(),
        test_start,
        rps_queue.clone(),
        api_rps_queue.clone(),
        queue_cap,
        last_total_sent.clone(),
        last_err.clone(),
        last_dura.clone(),
        engine_errors.clone(),
        verbose,
    ));

    // 等待所有连接任务退出
    let task_results = join_all(handles).await;
    for r in task_results {
        match r {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                engine_errors
                    .lock()
                    .push(format!("WS 任务内部错误: {:?}", e));
            }
            Err(e) => {
                engine_errors
                    .lock()
                    .push(format!("WS 任务被取消: {:?}", e));
            }
        }
    }

    // 等断言 worker 收尾
    for h in assert_workers {
        let _ = h.await;
    }

    // 全局 teardown
    if let Some(td) = teardown_options_rendered {
        let local = (*extract_map_arc).clone();
        if let Err(e) = setup::start_setup(td, local, http_client.clone()).await {
            engine_errors
                .lock()
                .push(format!("WS 全局 teardown 失败: {:?}", e));
        }
    }

    // 最终聚合
    let total_duration = (Instant::now() - test_start).as_secs_f64();
    let mut final_results = Vec::with_capacity(ws_stats_list.len());
    let api_rps_snapshot = api_rps_queue.lock().await.clone();
    for s in &ws_stats_list {
        let mut snap = share_ws_result::snapshot_ws_api_result(s, total_duration).await;
        let rps = match api_rps_snapshot.get(&snap.name) {
            Some(q) => q.average().await.unwrap_or(0.0),
            None => 0.0,
        };
        snap.rps = if rps > 0.0 {
            rps
        } else if total_duration > 0.0 {
            snap.total_requests as f64 / total_duration
        } else {
            0.0
        };
        final_results.push(snap);
    }

    let total_sent: u64 = final_results.iter().map(|r| r.total_requests).sum();
    let total_recv: u64 = final_results.iter().map(|r| r.messages_received).sum();
    let total_succ: u64 = final_results.iter().map(|r| r.successful_requests).sum();
    let total_err: i32 = final_results.iter().map(|r| r.err_count).sum();
    let total_bs: f64 = final_results.iter().map(|r| r.bytes_sent_kb).sum();
    let total_br: f64 = final_results.iter().map(|r| r.bytes_received_kb).sum();
    let total_opened: u64 = final_results.iter().map(|r| r.connections_opened).sum();
    let total_failed: u64 = final_results.iter().map(|r| r.connections_failed).sum();
    let total_dropped: u64 = final_results.iter().map(|r| r.connections_dropped).sum();
    let total_active: i32 = final_results.iter().map(|r| r.connections_active).sum();
    let total_reconn: u64 = final_results.iter().map(|r| r.reconnects).sum();
    let total_hbto: u64 = final_results.iter().map(|r| r.heartbeat_timeouts).sum();
    let total_match_to: u64 = final_results.iter().map(|r| r.match_timeouts).sum();
    let max_rt = final_results.iter().map(|r| r.max_response_time).max().unwrap_or(0);
    // per-endpoint min 已把无 RTT 数据的哨兵归零, 这里跳过 0 只对有数据的取最小
    let min_rt = final_results
        .iter()
        .map(|r| r.min_response_time)
        .filter(|&v| v > 0)
        .min()
        .unwrap_or(0);

    // 分母用"已判定单元数" = 成功 + 错误, 与逐秒快照口径一致
    let judged = total_succ + total_err.max(0) as u64;
    let success_rate = if judged > 0 { total_succ as f64 / judged as f64 * 100.0 } else { 0.0 };
    let error_rate = if judged > 0 { total_err as f64 / judged as f64 * 100.0 } else { 0.0 };
    let throughput = if total_duration > 0.0 { (total_bs + total_br) / total_duration } else { 0.0 };

    let (hp50, hp95, hp99) = share_ws_result::merged_percentiles(&ws_stats_list, true);
    let (rp50, rp95, rp99) = share_ws_result::merged_percentiles(&ws_stats_list, false);

    let avg_rt = if total_succ > 0 {
        let sum_rt: u64 = ws_stats_list
            .iter()
            .map(|s| s.total_response_time_ms.load(Ordering::Relaxed))
            .sum();
        (sum_rt as f64 / total_succ as f64).round() as u64
    } else {
        0
    };

    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);

    // 同上, 先把 parking_lot 锁内的 clone 提到 await 之外
    let ws_errors_snap = ws_errors.errors.lock().clone();
    let engine_errors_snap = engine_errors.lock().clone();
    let assert_errors_snap = assert_errors.errors.lock().clone();

    let result = WsBatchResult {
        total_duration,
        timestamp,
        total_connections_opened: total_opened,
        total_connections_failed: total_failed,
        total_connections_dropped: total_dropped,
        total_concurrent_number: total_active,
        total_reconnects: total_reconn,
        total_heartbeat_timeouts: total_hbto,
        handshake_p50_ms: hp50,
        handshake_p95_ms: hp95,
        handshake_p99_ms: hp99,
        total_messages_sent: total_sent,
        total_messages_received: total_recv,
        successful_requests: total_succ,
        success_rate,
        error_rate,
        err_count: total_err,
        errors_per_second: 0,
        rps: rps_queue.lock().await.average().await.unwrap_or(0.0),
        total_data_sent_kb: total_bs,
        total_data_received_kb: total_br,
        throughput_per_second_kb: throughput,
        median_response_time: rp50,
        response_time_95: rp95,
        response_time_99: rp99,
        max_response_time: max_rt,
        min_response_time: min_rt,
        avg_response_time: avg_rt,
        total_match_timeouts: total_match_to,
        ws_results: final_results,
        ws_errors: ws_errors_snap,
        assert_errors: assert_errors_snap,
        data_pool_stats: data_pool.as_ref().map(|dp| dp.get_stats()),
        engine_errors: engine_errors_snap,
    };
    let _ = stop_tx.send(());
    Ok(result)
}
