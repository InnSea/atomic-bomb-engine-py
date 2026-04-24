use crate::core::exponential_moving_average;
use crate::core::fixed_size_queue;
use crate::models::api_endpoint_stats::ApiEndpointStats;
use crate::models::assert_error_stats::AssertErrorStats;
use crate::models::http_error_stats::HttpErrorStats;
use crate::models::result::{ApiResult, BatchResult};
use histogram::AtomicHistogram;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::select;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot::Receiver;
use tokio::sync::Mutex;
use tokio::time::interval;
use url::Url;

/// 从 ApiEndpointStats 的原子计数器 + histogram 组装一个 ApiResult 快照
/// 在热路径之外调用(tick / 最终结果), 短锁 histogram 读三个 percentile 后立即释放
pub(crate) async fn snapshot_api_result(
    stats: &ApiEndpointStats,
    total_duration_secs: f64,
) -> ApiResult {
    // 先读所有原子(同一时刻的非原子一致视图, 可接受)
    let total_requests = stats.total_requests.load(Ordering::Relaxed) as u64;
    let successful = stats.successful_requests.load(Ordering::Relaxed) as u64;
    let err_count = stats.err_count.load(Ordering::Relaxed) as i32;
    let max_rt = stats.max_response_time.load(Ordering::Relaxed);
    let min_rt = stats.min_response_time.load(Ordering::Relaxed);
    let total_rt_ms = stats.total_response_time_ms.load(Ordering::Relaxed);
    let total_size_bytes = stats.total_response_size.load(Ordering::Relaxed);
    let concurrent = stats.concurrent_number.load(Ordering::Relaxed) as i32;

    let total_data_kb = total_size_bytes as f64 / 1024f64;
    let success_rate = if total_requests > 0 {
        successful as f64 / total_requests as f64 * 100.0
    } else {
        0.0
    };
    let error_rate = if total_requests > 0 {
        err_count as f64 / total_requests as f64 * 100.0
    } else {
        0.0
    };
    let avg_response_time = if total_requests > 0 {
        (total_rt_ms as f64 / total_requests as f64).round() as u64
    } else {
        0
    };
    let throughput_per_second_kb = if total_duration_secs > 0f64 {
        total_data_kb / total_duration_secs
    } else {
        0.0
    };

    // AtomicHistogram load() 生成 snapshot, 再读 percentile (无锁, 仅原子读)
    let (p50, p95, p99) = {
        let snapshot = stats.histogram.load();
        let p50 = snapshot
            .percentile(50.0)
            .map(|b| *b.range().start())
            .unwrap_or(0);
        let p95 = snapshot
            .percentile(95.0)
            .map(|b| *b.range().start())
            .unwrap_or(0);
        let p99 = snapshot
            .percentile(99.0)
            .map(|b| *b.range().start())
            .unwrap_or(0);
        (p50, p95, p99)
    };

    // host / path 从 url 解析(缓存在本地 struct 可选, 此处每 tick 一次开销可忽略)
    let (host, path) = match Url::parse(&stats.url) {
        Ok(u) => (
            u.host().map(|h| h.to_string()).unwrap_or_default(),
            u.path().to_string(),
        ),
        Err(_) => (String::new(), String::new()),
    };

    ApiResult {
        name: stats.name.clone(),
        url: stats.url.clone(),
        host,
        path,
        method: stats.method.clone(),
        success_rate,
        error_rate,
        median_response_time: p50,
        response_time_95: p95,
        response_time_99: p99,
        total_requests,
        rps: 0.0, // rps 由 tick 计算后覆盖
        max_response_time: max_rt,
        min_response_time: min_rt,
        err_count,
        total_data_kb,
        throughput_per_second_kb,
        concurrent_number: concurrent,
        avg_response_time,
    }
}

pub(crate) async fn collect_results(
    result_channel: Sender<Option<BatchResult>>,
    should_stop_rx: Receiver<()>,
    total_requests: Arc<AtomicUsize>,
    successful_requests: Arc<AtomicUsize>,
    histogram: Arc<AtomicHistogram>,
    total_response_size: Arc<AtomicUsize>,
    total_response_time_ms: Arc<AtomicU64>,
    http_errors: Arc<Mutex<HttpErrorStats>>,
    err_count: Arc<AtomicUsize>,
    max_resp_time: Arc<AtomicU64>,
    min_resp_time: Arc<AtomicU64>,
    assert_error: Arc<Mutex<AssertErrorStats>>,
    api_endpoint_stats: Vec<Arc<ApiEndpointStats>>,
    concurrent_number: Arc<AtomicUsize>,
    dura: Arc<Mutex<f64>>,
    number_of_last_requests: Arc<AtomicUsize>,
    number_of_last_errors: Arc<AtomicUsize>,
    rps_queue: Arc<Mutex<fixed_size_queue::FixedSizeQueue<f64>>>,
    api_rps_queue_arc: Arc<Mutex<BTreeMap<String, fixed_size_queue::FixedSizeQueue<f64>>>>,
    queue_cap: usize,
    verbose: bool,
    test_start: Instant,
    ema_alpha: f64,
    engine_errors: Arc<Mutex<Vec<String>>>,
) {
    let mut api_res_number_map: HashMap<String, usize> = HashMap::new();
    let mut interval = interval(Duration::from_secs(1));
    let mut api_rps_queue_map = api_rps_queue_arc.lock().await.clone();
    let ema: Option<exponential_moving_average::ExponentialMovingAverage> =
        if ema_alpha > 0f64 {
            Some(exponential_moving_average::ExponentialMovingAverage::new(
                ema_alpha,
            ))
        } else {
            None
        };
    select! {
        _ = should_stop_rx => {
            println!("收到停止信号");
            return;
        }
         _ = async {
            loop{
                interval.tick().await;
                let err_count = err_count.load(Ordering::SeqCst) as i32;
                let max_response_time_c = max_resp_time.load(Ordering::SeqCst);
                let min_response_time_c = min_resp_time.load(Ordering::SeqCst);
                let total_duration = (Instant::now() - test_start).as_secs_f64();
                let mut d = dura.lock().await;
                let this_duration = total_duration - *d;
                *d = total_duration;
                drop(d);
                let total_requests = total_requests.load(Ordering::SeqCst) as f64;
                let successful_requests = successful_requests.load(Ordering::SeqCst) as f64;
                let success_rate = if total_requests == 0f64 {
                    0f64
                } else {
                    successful_requests / total_requests * 100.0
                };
                let error_rate = if total_requests == 0f64 {
                    0f64
                } else {
                    err_count as f64 / total_requests * 100.0
                };
                let total_response_size_kb = total_response_size.load(Ordering::SeqCst) as f64 / 1024.0;
                let throughput_kb_s = if total_duration > 0f64 { total_response_size_kb / total_duration } else { 0.0 };
                // AtomicHistogram load 生成 snapshot, 再读 percentile (无锁)
                let (resp_median_line, resp_95_line, resp_99_line) = {
                    let snapshot = histogram.load();
                    let p50 = snapshot.percentile(50.0).map(|b| *b.range().start()).unwrap_or(0);
                    let p95 = snapshot.percentile(95.0).map(|b| *b.range().start()).unwrap_or(0);
                    let p99 = snapshot.percentile(99.0).map(|b| *b.range().start()).unwrap_or(0);
                    (p50, p95, p99)
                };
                // 短锁: 只取 errors Arc
                let http_errors = http_errors.lock().await.errors.clone();
                let assert_errors = assert_error.lock().await.errors.clone();
                let timestamp = match SystemTime::now().duration_since(UNIX_EPOCH) {
                    Ok(n) => n.as_millis(),
                    Err(_) => 0,
                };
                // 组装每个接口的 ApiResult: 完全从 atomic 读出, 无共享锁
                let mut api_results_local: Vec<ApiResult> = Vec::with_capacity(api_endpoint_stats.len());
                for stats in api_endpoint_stats.iter() {
                    let snap = snapshot_api_result(stats.as_ref(), total_duration).await;
                    api_results_local.push(snap);
                }
                // 计算每个接口的 rps
                for (index, res) in api_results_local.clone().into_iter().enumerate() {
                    let api_latest_request_number = *api_res_number_map.get(&res.name).unwrap_or(&0);
                    let api_requests_per_second = res.total_requests as usize - api_latest_request_number;
                    api_res_number_map.insert(res.name.clone(), api_requests_per_second + api_latest_request_number);
                    let mut rps = if this_duration > 0f64 { api_requests_per_second as f64 / this_duration } else { 0.0 };
                    match api_rps_queue_map.get_mut(&res.name){
                        None => {
                            api_rps_queue_map.insert(res.name.clone(), fixed_size_queue::FixedSizeQueue::new(queue_cap));
                            if let Some(queue) = api_rps_queue_map.get_mut(&res.name){
                                queue.push(rps).await
                            };
                        }
                        Some(queue) => {
                            queue.push(rps).await
                        }
                    }
                    rps = match ema.clone(){
                        None => rps,
                        Some(mut e) => e.add(rps)
                    };
                    api_results_local[index].rps = rps;
                }
                let total_concurrent_number = concurrent_number.load(Ordering::SeqCst) as i32;
                let errors_per_second = err_count as usize - number_of_last_errors.load(Ordering::SeqCst);
                number_of_last_errors.fetch_add(errors_per_second, Ordering::Relaxed);
                let requests_per_second = total_requests as usize - number_of_last_requests.load(Ordering::SeqCst);
                number_of_last_requests.fetch_add(requests_per_second, Ordering::Relaxed);
                let mut rps = if this_duration > 0f64 { requests_per_second as f64 / this_duration } else { 0.0 };
                rps = match ema.clone(){
                    None => rps,
                    Some(mut e) => e.add(rps)
                };
                {
                    let mut rps_queue = rps_queue.lock().await;
                    rps_queue.push(rps).await;
                }
                let result = BatchResult {
                    total_duration,
                    success_rate,
                    error_rate,
                    median_response_time: resp_median_line,
                    response_time_95: resp_95_line,
                    response_time_99: resp_99_line,
                    total_requests: total_requests as u64,
                    rps,
                    max_response_time: max_response_time_c,
                    min_response_time: min_response_time_c,
                    err_count,
                    total_data_kb: total_response_size_kb,
                    throughput_per_second_kb: throughput_kb_s,
                    http_errors: http_errors.lock().await.clone(),
                    timestamp,
                    assert_errors: assert_errors.lock().await.clone(),
                    total_concurrent_number,
                    api_results: api_results_local.clone(),
                    errors_per_second,
                    data_pool_stats: None,
                    avg_response_time: if total_requests > 0f64 {
                        (total_response_time_ms.load(Ordering::SeqCst) as f64 / total_requests).round() as u64
                    } else {
                        0
                    },
                    engine_errors: {
                        let mut errs = engine_errors.lock().await;
                        let snapshot = errs.clone();
                        errs.clear();
                        snapshot
                    },
                };
                let elapsed = test_start.elapsed();
                if verbose {
                    println!("{:?}-{:#?}", elapsed.as_millis(), result.clone());
                };
                let _ = result_channel.send(Some(result)).await;
            }
        } => {}
    }
}
