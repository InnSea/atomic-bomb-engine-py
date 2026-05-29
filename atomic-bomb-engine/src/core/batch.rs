use std::collections::BTreeMap;
use std::env;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Error;
use futures::future::join_all;
use handlebars::Handlebars;
use histogram::AtomicHistogram;
use reqwest::header::{HeaderMap, HeaderValue, USER_AGENT};
use reqwest::Client;
use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task::JoinHandle;

use crate::core::check_endpoints_names::check_endpoints_names;
use crate::core::concurrency_controller::ConcurrencyController;
use crate::core::fixed_size_queue;
use crate::core::sleep_guard::SleepGuard;
use crate::core::{listening_assert, setup, share_result, start_task};
use crate::models::api_endpoint::ApiEndpoint;
use crate::models::api_endpoint_stats::ApiEndpointStats;
use crate::models::assert_error_stats::AssertErrorStats;
use crate::models::data_pool::DataPool;
use crate::models::http_error_stats::HttpErrorStats;
use crate::models::result::BatchResult;
use crate::models::setup::SetupApiEndpoint;
use crate::models::step_option::{InnerStepOption, StepOption};

/// 断言消费 worker 数量. 足够吃满 JSONPath 解析的 CPU, 同时避免过度调度
const ASSERT_WORKER_COUNT: usize = 4;

fn render_global_setup_urls_once(
    options: Option<Vec<SetupApiEndpoint>>,
    handlebars: &Handlebars,
    context: &BTreeMap<String, Value>,
    phase: &str,
) -> anyhow::Result<Option<Vec<SetupApiEndpoint>>> {
    let Some(mut opts) = options else {
        return Ok(None);
    };
    for item in &mut opts {
        let rendered = match handlebars.render_template(&item.url, &json!(context)) {
            Ok(url) => url,
            Err(e) => {
                return Err(Error::msg(format!(
                    "{} URL模板渲染失败, name: {:?}, url: {:?}, err: {:?}",
                    phase, item.name, item.url, e
                )));
            }
        };
        item.url = rendered;
    }
    Ok(Some(opts))
}

pub async fn batch(
    result_sender: mpsc::Sender<Option<BatchResult>>,
    test_duration_secs: u64,
    concurrent_requests: usize,
    timeout_secs: u64,
    cookie_store_enable: bool,
    verbose: bool,
    should_prevent: bool,
    api_endpoints: Vec<ApiEndpoint>,
    step_option: Option<StepOption>,
    setup_options: Option<Vec<SetupApiEndpoint>>,
    mut assert_channel_buffer_size: usize,
    ema_alpha: f64,
    should_stop: Option<Arc<std::sync::atomic::AtomicBool>>,
    data_pool: Option<Arc<DataPool>>,
    global_variables: Option<BTreeMap<String, Value>>,
    teardown_options: Option<Vec<SetupApiEndpoint>>,
) -> anyhow::Result<BatchResult> {
    // 阻止电脑休眠
    let _guard = SleepGuard::new(should_prevent);
    // 检查阶梯并发量
    if let Some(step_option) = step_option.clone() {
        // 计算总共增加次数
        let total_steps = test_duration_secs / step_option.increase_interval;
        // 计算总增加并发数
        let total_concurrency_increase =
            step_option.increase_step as u64 * total_steps * (total_steps + 1) / 2;
        if total_concurrency_increase < concurrent_requests as u64 {
            return Err(Error::msg(
                "阶梯加压总并发数在设置的时间内无法增加到预设的结束并发数",
            ));
        }
    };
    // 检查每个接口的名称
    if let Err(e) = check_endpoints_names(api_endpoints.clone()) {
        return Err(Error::msg(e));
    }
    // 总响应时间统计 (AtomicHistogram 内部用原子 bucket, 热路径无锁 increment)
    let histogram = match AtomicHistogram::new(14, 20) {
        Ok(h) => Arc::new(h),
        Err(e) => {
            return Err(Error::msg(format!("获取存储桶失败::{:?}", e.to_string())));
        }
    };
    // 成功数据统计
    let successful_requests = Arc::new(AtomicUsize::new(0));
    // 请求总数统计
    let total_requests = Arc::new(AtomicUsize::new(0));
    // 统计最大响应时间
    let max_response_time = Arc::new(AtomicU64::new(0));
    // 统计最小响应时间
    let min_response_time = Arc::new(AtomicU64::new(u64::MAX));
    // 统计总响应时间（用于计算平均响应时间）
    let total_response_time_ms = Arc::new(AtomicU64::new(0));
    // 统计错误数量
    let err_count = Arc::new(AtomicUsize::new(0));
    // 统计每秒错误数
    let number_of_last_errors = Arc::new(AtomicUsize::new(0));
    let dura = Arc::new(Mutex::new(0f64));
    // 统计rps
    let number_of_last_requests = Arc::new(AtomicUsize::new(0));
    // rps队列
    // 计算队列长度
    let queue_cap = match step_option.clone() {
        // 没有阶梯加压，队列长度为10
        None => 10usize,
        // 有阶梯加压，计算出最大并发持续时间，变为队列长度
        Some(step_option) => {
            // 计算最大并发量所需时间
            let steps_to_max_concurrency = concurrent_requests / step_option.increase_step;
            let time_to_max_concurrency =
                steps_to_max_concurrency as u64 * step_option.increase_interval;
            // 计算最大并发量剩余时间
            let remaining_time = test_duration_secs.saturating_sub(time_to_max_concurrency);
            remaining_time as usize
        }
    };
    let rps_queue_arc = Arc::new(Mutex::new(fixed_size_queue::FixedSizeQueue::new(queue_cap)));
    // api rps队列
    let api_rps_queue_arc: Arc<Mutex<BTreeMap<String, fixed_size_queue::FixedSizeQueue<f64>>>> =
        Arc::new(Mutex::new(BTreeMap::new()));
    // 已开始并发数
    let concurrent_number = Arc::new(AtomicUsize::new(0));
    // 接口线程池
    let mut handles: Vec<JoinHandle<Result<(), Error>>> = Vec::new();
    // 统计响应大小
    let total_response_size = Arc::new(AtomicUsize::new(0));
    // 统计http错误
    let http_errors = Arc::new(Mutex::new(HttpErrorStats::new()));
    // 统计断言错误
    let assert_errors = AssertErrorStats::new();
    // 引擎错误收集
    let engine_errors: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    // 总权重
    let total_weight: u32 = api_endpoints.iter().map(|e| e.weight).sum();
    // 是否停止通道
    let (should_stop_tx, should_stop_rx) = oneshot::channel();
    // 断言队列
    if assert_channel_buffer_size <= 0 {
        assert_channel_buffer_size = 1024
    }
    let (tx_assert, rx_assert) = mpsc::channel(assert_channel_buffer_size);
    // 是否有接口配置了断言
    let has_assert = api_endpoints
        .iter()
        .any(|item| item.assert_options.is_some());
    // 开启 N 个并发 worker 消费断言队列 (MPMC, 不再 oneshot 等待)
    let assert_worker_handles: Vec<tokio::task::JoinHandle<()>> = if has_assert {
        if verbose {
            println!("开启 {} 个断言消费 worker", ASSERT_WORKER_COUNT);
        };
        listening_assert::spawn_assert_workers(rx_assert, ASSERT_WORKER_COUNT)
    } else {
        // 没有断言需求时, 立即关闭 receiver 防止泄漏
        drop(rx_assert);
        Vec::new()
    };
    // endpoint 在初始化阶段完成 url 模板渲染后冻结为 Arc<ApiEndpoint>
    // 开始测试时间
    let test_start = Instant::now();
    // 测试结束时间
    let test_end = test_start + Duration::from_secs(test_duration_secs);
    // user_agent
    let info = os_info::get();
    let os_type = info.os_type();
    let os_version = info.version().to_string();
    let app_name = env!("CARGO_PKG_NAME");
    let app_version = env!("CARGO_PKG_VERSION");
    let user_agent_value =
        match format!("{} {} ({}; {})", app_name, app_version, os_type, os_version)
            .parse::<HeaderValue>()
        {
            Ok(v) => v,
            Err(e) => {
                return Err(Error::msg(format!(
                    "解析user agent失败::{:?}",
                    e.to_string()
                )));
            }
        };
    let mut is_need_render_template = false;
    // 全局提取字典
    let mut extract_map: BTreeMap<String, Value> = BTreeMap::new();
    // 注入全局变量
    if let Some(vars) = global_variables {
        extract_map.extend(vars);
        is_need_render_template = true;
    }
    // 数据池
    let data_pool_arc = data_pool.clone();
    // 如果有数据池，设置需要渲染模板
    if data_pool_arc.is_some() {
        is_need_render_template = true;
    }
    // 停止信号
    let should_stop_flag = should_stop.unwrap_or_else(|| Arc::new(AtomicBool::new(false)));
    // 复用Handlebars实例
    let handlebars = Handlebars::new();
    // 仅使用全局变量做一次性URL渲染（不依赖运行时提取值）
    let global_url_render_context = extract_map.clone();
    let setup_options = render_global_setup_urls_once(
        setup_options,
        &handlebars,
        &global_url_render_context,
        "全局setup",
    )?;
    let teardown_options = render_global_setup_urls_once(
        teardown_options,
        &handlebars,
        &global_url_render_context,
        "全局teardown",
    )?;
    // 创建http客户端
    let builder = Client::builder()
        .cookie_store(cookie_store_enable)
        .default_headers({
            let mut headers = HeaderMap::new();
            headers.insert(USER_AGENT, user_agent_value);
            headers
        });
    let client = match timeout_secs > 0 {
        true => match builder.timeout(Duration::from_secs(timeout_secs)).build() {
            Ok(cli) => cli,
            Err(e) => return Err(Error::msg(format!("构建含有超时的http客户端失败: {:?}", e))),
        },
        false => match builder.build() {
            Ok(cli) => cli,
            Err(e) => return Err(Error::msg(format!("构建http客户端失败: {:?}", e))),
        },
    };
    // 开始初始化
    if let Some(setup_options) = setup_options {
        is_need_render_template = true;
        match setup::start_setup(setup_options, extract_map.clone(), client.clone()).await {
            Ok(res) => {
                if let Some(extract) = res {
                    extract_map.extend(extract);
                };
            }
            Err(e) => return Err(Error::msg(format!("全局初始化失败: {:?}", e))),
        };
    };
    // println!("extract_map:{:?}", extract_map);
    // 并发安全的提取字典（setup后只读，不需要Mutex）
    let extract_map_arc = Arc::new(extract_map);
    // 收集每个 endpoint 的 stats, 让 collect_results / 最终结果从原子快照组装 ApiResult
    let mut api_endpoint_stats: Vec<Arc<ApiEndpointStats>> = Vec::new();
    // 针对每一个接口开始配置
    for (_index, mut endpoint) in api_endpoints.into_iter().enumerate() {
        let weight = endpoint.weight;
        let name = endpoint.name.clone();
        let api_url = match is_need_render_template {
            true => match handlebars.render_template(&endpoint.url, &json!(*extract_map_arc)) {
                Ok(c) => c,
                Err(e) => {
                    engine_errors
                        .lock()
                        .await
                        .push(format!("URL模板渲染失败: {:?}", e));
                    endpoint.url.clone()
                }
            },
            false => endpoint.url.clone(),
        };
        // 将渲染后的 url 写回 endpoint, 之后全程只读
        endpoint.url = api_url.clone();
        // 计算权重比例
        let weight_ratio = weight as f64 / total_weight as f64;
        // 计算每个接口的并发量
        let mut concurrency_for_endpoint =
            ((concurrent_requests as f64) * weight_ratio).round() as usize;
        if concurrency_for_endpoint == 0 {
            concurrency_for_endpoint = 1
        }
        // 接口 histogram (AtomicHistogram, 无锁)
        let api_histogram = match AtomicHistogram::new(14, 20) {
            Ok(h) => Arc::new(h),
            Err(e) => return Err(Error::msg(format!("获取存储桶失败::{:?}", e.to_string()))),
        };
        // 所有 per-endpoint 统计合并到 ApiEndpointStats, 热路径只做原子操作 + histogram 短锁
        let stats = Arc::new(ApiEndpointStats::new(
            name.clone(),
            api_url.clone(),
            endpoint.method.clone().to_uppercase(),
            api_histogram,
        ));
        api_endpoint_stats.push(Arc::clone(&stats));
        // 根据 step 初始化并发控制器
        let controller = match step_option.clone() {
            None => Arc::new(ConcurrencyController::new(concurrency_for_endpoint, None)),
            Some(option) => {
                let step = option.increase_step as f64 * weight_ratio;
                Arc::new(ConcurrencyController::new(
                    concurrency_for_endpoint,
                    Option::from(InnerStepOption {
                        increase_step: step,
                        increase_interval: option.increase_interval,
                    }),
                ))
            }
        };
        tokio::spawn({
            let controller_clone = Arc::clone(&controller);
            async move {
                controller_clone.distribute_permits().await;
            }
        });
        // 冻结 endpoint 为只读 Arc
        let endpoint_arc: Arc<ApiEndpoint> = Arc::new(endpoint);
        for _ in 0..concurrency_for_endpoint {
            let handle: JoinHandle<Result<(), Error>> =
                tokio::spawn(start_task::start_concurrency(
                    client.clone(),
                    Arc::clone(&controller),
                    Arc::clone(&concurrent_number),
                    Arc::clone(&extract_map_arc),
                    Arc::clone(&endpoint_arc),
                    Arc::clone(&stats),
                    Arc::clone(&total_requests),
                    Arc::clone(&histogram),
                    Arc::clone(&max_response_time),
                    Arc::clone(&min_response_time),
                    Arc::clone(&total_response_size),
                    Arc::clone(&total_response_time_ms),
                    Arc::clone(&successful_requests),
                    Arc::clone(&err_count),
                    Arc::clone(&http_errors),
                    assert_errors.clone(),
                    tx_assert.clone(),
                    test_start,
                    test_end,
                    is_need_render_template,
                    verbose,
                    Arc::clone(&should_stop_flag),
                    data_pool_arc.clone(),
                    Arc::clone(&engine_errors),
                ));
            handles.push(handle);
        }
    }
    // 主 batch 不再持有发送端, 让 worker 侧的 drop 更确定地传播关闭信号
    drop(tx_assert);

    // 共享任务状态: collect_results 每秒 tick 时从 api_endpoint_stats 的原子+histogram 快照组装 ApiResult
    tokio::spawn(share_result::collect_results(
        result_sender,
        should_stop_rx,
        Arc::clone(&total_requests),
        Arc::clone(&successful_requests),
        Arc::clone(&histogram),
        Arc::clone(&total_response_size),
        Arc::clone(&total_response_time_ms),
        Arc::clone(&http_errors),
        Arc::clone(&err_count),
        Arc::clone(&max_response_time),
        Arc::clone(&min_response_time),
        assert_errors.clone(),
        api_endpoint_stats.clone(),
        Arc::clone(&concurrent_number),
        Arc::clone(&dura),
        Arc::clone(&number_of_last_requests),
        Arc::clone(&number_of_last_errors),
        Arc::clone(&rps_queue_arc),
        Arc::clone(&api_rps_queue_arc),
        queue_cap,
        verbose,
        test_start,
        ema_alpha,
        Arc::clone(&engine_errors),
    ));

    // 等待任务完成
    let task_results = join_all(handles).await;
    for task_result in task_results {
        match task_result {
            Ok(res) => {
                match res {
                    Ok(_) => {
                        if verbose {
                            println!("任务完成")
                        }
                    }
                    Err(e) => {
                        let err_msg = format!("异步任务内部错误::{:?}", e);
                        engine_errors.lock().await.push(err_msg);
                    }
                };
            }
            Err(err) => {
                let err_msg = format!("协程被取消或意外停止::{:?}", err);
                engine_errors.lock().await.push(err_msg);
            }
        };
    }
    // 此刻所有 task 都已退出, 它们持有的 tx_assert 全部释放 → 断言 channel 关闭
    // 等待 N 个 assert worker 把队列里剩余的任务消化干净, 保证最终统计不丢
    if !assert_worker_handles.is_empty() {
        for h in assert_worker_handles {
            let _ = h.await;
        }
    }

    // 执行全局teardown
    if let Some(teardown_opts) = teardown_options {
        let teardown_extract_map = (*extract_map_arc).clone();
        match setup::start_setup(teardown_opts, teardown_extract_map, client.clone()).await {
            Ok(_) => {
                if verbose {
                    println!("全局teardown执行完成");
                }
            }
            Err(e) => {
                let err_msg = format!("全局teardown执行失败: {:?}", e);
                engine_errors.lock().await.push(err_msg);
            }
        }
    }

    // 最终结果: 完全从原子 + histogram 快照组装, 不再依赖中间 results_arc
    let err_count_final = err_count.load(Ordering::SeqCst);
    let total_duration = (Instant::now() - test_start).as_secs_f64();
    let total_requests_final = total_requests.load(Ordering::SeqCst) as u64;
    let successful_requests_final = successful_requests.load(Ordering::SeqCst) as f64;
    let success_rate = if total_requests_final > 0 {
        successful_requests_final / total_requests_final as f64 * 100.0
    } else {
        0.0
    };
    let error_rate = if total_requests_final > 0 {
        err_count_final as f64 / total_requests_final as f64 * 100.0
    } else {
        0.0
    };
    let total_response_size_kb = total_response_size.load(Ordering::SeqCst) as f64 / 1024.0;
    let throughput_kb_s = total_response_size_kb / test_duration_secs as f64;
    let http_errors_snapshot = http_errors.lock().await.errors.clone();
    let assert_errors_snapshot = assert_errors.errors.clone();
    let timestamp = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(n) => n.as_millis(),
        Err(_) => 0,
    };
    // 组装每个接口的 ApiResult: 从原子 + histogram 快照, 再用 rps_queue 覆盖 rps
    let api_rps_queue_snapshot = api_rps_queue_arc.lock().await.clone();
    let mut api_results_final = Vec::with_capacity(api_endpoint_stats.len());
    for stats in api_endpoint_stats.iter() {
        let mut snap = share_result::snapshot_api_result(stats.as_ref(), total_duration).await;
        // 优先用 rps_queue 的平滑平均; 拿不到再 fallback 到 total / duration
        let rps = match api_rps_queue_snapshot.get(&snap.name) {
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
        api_results_final.push(snap);
    }
    let total_concurrent_number_final = concurrent_number.load(Ordering::SeqCst) as i32;
    let errors_per_second = err_count_final - number_of_last_errors.load(Ordering::SeqCst);
    number_of_last_errors.fetch_add(errors_per_second, Ordering::Relaxed);
    let rps = rps_queue_arc
        .lock()
        .await
        .average()
        .await
        .unwrap_or_else(|| 0f64);
    number_of_last_requests.fetch_add(rps as usize, Ordering::Relaxed);
    let data_pool_stats = data_pool.as_ref().map(|dp| dp.get_stats());
    // AtomicHistogram 无锁 snapshot, 再读三个 percentile
    let (median_response_time, response_time_95, response_time_99) = {
        let snapshot = histogram.load();
        let p50 = match snapshot.percentile(50.0) {
            Ok(b) => *b.range().start(),
            Err(e) => return Err(Error::msg(format!("获取50线失败::{:?}", e.to_string()))),
        };
        let p95 = match snapshot.percentile(95.0) {
            Ok(b) => *b.range().start(),
            Err(e) => return Err(Error::msg(format!("获取95线失败::{:?}", e.to_string()))),
        };
        let p99 = match snapshot.percentile(99.0) {
            Ok(b) => *b.range().start(),
            Err(e) => return Err(Error::msg(format!("获取99线失败::{:?}", e.to_string()))),
        };
        (p50, p95, p99)
    };
    // parking_lot guard 不是 Send, 跨 await 会让 spawn 不能调度. 把锁内 clone 提到 await 之前.
    let assert_errors_final = assert_errors_snapshot.lock().clone();
    let result = Ok(BatchResult {
        total_duration,
        success_rate,
        error_rate,
        median_response_time,
        response_time_95,
        response_time_99,
        total_requests: total_requests_final,
        rps,
        max_response_time: max_response_time.load(Ordering::SeqCst),
        min_response_time: min_response_time.load(Ordering::SeqCst),
        err_count: err_count_final as i32,
        total_data_kb: total_response_size_kb,
        throughput_per_second_kb: throughput_kb_s,
        http_errors: http_errors_snapshot.lock().await.clone(),
        timestamp,
        assert_errors: assert_errors_final,
        total_concurrent_number: total_concurrent_number_final,
        api_results: api_results_final,
        errors_per_second,
        data_pool_stats,
        avg_response_time: if total_requests_final > 0 {
            (total_response_time_ms.load(Ordering::SeqCst) as f64
                / total_requests_final as f64)
                .round() as u64
        } else {
            0
        },
        engine_errors: engine_errors.lock().await.clone(),
    });
    should_stop_tx.send(()).unwrap();
    result
}

#[cfg(test)]
mod tests {
    use super::render_global_setup_urls_once;
    use crate::models::setup::SetupApiEndpoint;
    use handlebars::Handlebars;
    use serde_json::{json, Value};
    use std::collections::BTreeMap;

    fn build_setup(url: &str) -> SetupApiEndpoint {
        SetupApiEndpoint {
            name: "global-setup".to_string(),
            url: url.to_string(),
            method: "GET".to_string(),
            json: None,
            form_data: None,
            multipart_options: None,
            headers: None,
            cookies: None,
            jsonpath_extract: None,
        }
    }

    #[test]
    fn test_render_global_setup_urls_once_success() {
        let mut context = BTreeMap::<String, Value>::new();
        context.insert("base".to_string(), json!("http://127.0.0.1:8080"));
        let options = Some(vec![build_setup("{{base}}/setup")]);
        let handlebars = Handlebars::new();

        let rendered = render_global_setup_urls_once(options, &handlebars, &context, "全局setup")
            .expect("should render url")
            .expect("options should exist");

        assert_eq!(rendered[0].url, "http://127.0.0.1:8080/setup");
    }

    #[test]
    fn test_render_global_setup_urls_once_missing_key_renders_empty() {
        let context = BTreeMap::<String, Value>::new();
        let options = Some(vec![build_setup("{{base}}/setup")]);
        let handlebars = Handlebars::new();

        let rendered = render_global_setup_urls_once(options, &handlebars, &context, "全局setup")
            .expect("render should not fail in non-strict mode")
            .expect("options should exist");

        assert_eq!(rendered[0].url, "/setup");
    }
}
