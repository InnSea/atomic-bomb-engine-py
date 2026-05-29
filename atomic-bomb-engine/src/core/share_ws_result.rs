use crate::core::fixed_size_queue;
use crate::models::assert_error_stats::AssertErrorStats;
use crate::models::ws_endpoint_stats::WsEndpointStats;
use crate::models::ws_error_stats::WsErrorStats;
use crate::models::ws_result::{WsApiResult, WsBatchResult};
use histogram::{AtomicHistogram, Histogram};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use parking_lot::Mutex as PlMutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::select;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot::Receiver;
use tokio::sync::Mutex;
use tokio::time::interval;
use url::Url;

fn percentiles(h: &AtomicHistogram) -> (u64, u64, u64) {
    let snap = h.load();
    percentiles_of(&snap)
}

fn percentiles_of(snap: &Histogram) -> (u64, u64, u64) {
    let p50 = snap.percentile(50.0).map(|b| *b.range().start()).unwrap_or(0);
    let p95 = snap.percentile(95.0).map(|b| *b.range().start()).unwrap_or(0);
    let p99 = snap.percentile(99.0).map(|b| *b.range().start()).unwrap_or(0);
    (p50, p95, p99)
}

/// 把多个 endpoint 的直方图 snapshot 合并成一个, 用于全局百分位.
/// `select` 从每条 stats 取出对应直方图 (handshake 或 rtt).
fn merge_histograms<F>(stats_list: &[Arc<WsEndpointStats>], select: F) -> Option<Histogram>
where
    F: Fn(&WsEndpointStats) -> &AtomicHistogram,
{
    let mut iter = stats_list.iter();
    let first = iter.next()?;
    let mut acc = select(first).load();
    for s in iter {
        let snap = select(s).load();
        match acc.wrapping_add(&snap) {
            Ok(merged) => acc = merged,
            // 配置不同时 merge 会失败, 这里所有 endpoint 用同一对参数构造, 不会发生
            Err(_) => return Some(acc),
        }
    }
    Some(acc)
}
 
/// 合并所有 endpoint 的 handshake / rtt 直方图, 返回全局 (p50, p95, p99).
pub(crate) fn merged_percentiles(
    stats_list: &[Arc<WsEndpointStats>],
    is_handshake: bool,
) -> (u64, u64, u64) {
    let merged = if is_handshake {
        merge_histograms(stats_list, |s| s.handshake_histogram.as_ref())
    } else {
        merge_histograms(stats_list, |s| s.rtt_histogram.as_ref())
    };
    match merged {
        Some(h) => percentiles_of(&h),
        None => (0, 0, 0),
    }
}

pub(crate) async fn snapshot_ws_api_result(
    stats: &WsEndpointStats,
    total_duration_secs: f64,
) -> WsApiResult {
    let opened = stats.connections_opened.load(Ordering::Relaxed) as u64;
    let active = stats.concurrent_number.load(Ordering::Relaxed) as i32;
    let failed = stats.connections_failed.load(Ordering::Relaxed) as u64;
    let dropped = stats.connections_dropped.load(Ordering::Relaxed) as u64;
    let hb_to = stats.heartbeat_timeouts.load(Ordering::Relaxed) as u64;
    let reconn = stats.reconnects.load(Ordering::Relaxed) as u64;

    let sent = stats.messages_sent.load(Ordering::Relaxed) as u64;
    let received = stats.messages_received.load(Ordering::Relaxed) as u64;
    let succ = stats.successful_requests.load(Ordering::Relaxed) as u64;
    let err = stats.err_count.load(Ordering::Relaxed) as i32;
    let bs = stats.bytes_sent.load(Ordering::Relaxed) as f64 / 1024.0;
    let br = stats.bytes_received.load(Ordering::Relaxed) as f64 / 1024.0;
    let mt = stats.match_timeouts.load(Ordering::Relaxed) as u64;

    let total_rt_ms = stats.total_response_time_ms.load(Ordering::Relaxed);
    let max_rt = stats.max_response_time.load(Ordering::Relaxed);
    // min_response_time 初始为 u64::MAX, 仅 RequestResponse 配对成功才更新.
    // OneWay 模式从不更新, 需在快照时把哨兵值归零, 避免把脏值透传给前端.
    let min_rt = {
        let v = stats.min_response_time.load(Ordering::Relaxed);
        if v == u64::MAX { 0 } else { v }
    };

    // success_rate / error_rate 的分母用"已判定单元数" = 成功 + 错误,
    // 而非发送数. OneWay 模式下成功来自接收侧, 与发送数不同量纲,
    // 用 sent 做分母会算出 >100% 的荒谬比率.
    let judged = succ + err.max(0) as u64;
    let success_rate = if judged > 0 { succ as f64 / judged as f64 * 100.0 } else { 0.0 };
    let error_rate = if judged > 0 { err as f64 / judged as f64 * 100.0 } else { 0.0 };
    let avg_rt = if succ > 0 { (total_rt_ms as f64 / succ as f64).round() as u64 } else { 0 };

    let (hp50, hp95, hp99) = percentiles(&stats.handshake_histogram);
    let (rp50, rp95, rp99) = percentiles(&stats.rtt_histogram);

    let throughput = if total_duration_secs > 0.0 { (bs + br) / total_duration_secs } else { 0.0 };

    let (host, path) = match Url::parse(&stats.url) {
        Ok(u) => (
            u.host().map(|h| h.to_string()).unwrap_or_default(),
            u.path().to_string(),
        ),
        Err(_) => (String::new(), String::new()),
    };

    WsApiResult {
        name: stats.name.clone(),
        url: stats.url.clone(),
        host,
        path,
        connections_opened: opened,
        connections_active: active,
        connections_failed: failed,
        connections_dropped: dropped,
        heartbeat_timeouts: hb_to,
        reconnects: reconn,
        handshake_p50_ms: hp50,
        handshake_p95_ms: hp95,
        handshake_p99_ms: hp99,
        total_requests: sent,
        messages_received: received,
        successful_requests: succ,
        success_rate,
        error_rate,
        err_count: err,
        rps: 0.0,
        bytes_sent_kb: bs,
        bytes_received_kb: br,
        throughput_per_second_kb: throughput,
        median_response_time: rp50,
        response_time_95: rp95,
        response_time_99: rp99,
        max_response_time: max_rt,
        min_response_time: min_rt,
        avg_response_time: avg_rt,
        match_timeouts: mt,
    }
}

/// WS 实时聚合器, 每秒推送 WsBatchResult
pub(crate) async fn collect_ws_results(
    sender: Sender<Option<WsBatchResult>>,
    should_stop_rx: Receiver<()>,
    ws_stats_list: Vec<Arc<WsEndpointStats>>,
    ws_errors: Arc<WsErrorStats>,
    assert_errors: AssertErrorStats,
    test_start: Instant,
    rps_queue: Arc<Mutex<fixed_size_queue::FixedSizeQueue<f64>>>,
    api_rps_queue: Arc<Mutex<BTreeMap<String, fixed_size_queue::FixedSizeQueue<f64>>>>,
    queue_cap: usize,
    last_total_sent: Arc<AtomicUsize>,
    last_err: Arc<AtomicUsize>,
    last_dura: Arc<Mutex<f64>>,
    engine_errors: Arc<PlMutex<Vec<String>>>,
    verbose: bool,
) {
    let mut api_last_sent: HashMap<String, u64> = HashMap::new();
    let mut api_rps_map = api_rps_queue.lock().await.clone();
    let mut ticker = interval(Duration::from_secs(1));

    select! {
        _ = should_stop_rx => {
            if verbose { println!("WS collector 收到停止"); }
            return;
        }
        _ = async {
            loop {
                ticker.tick().await;
                let total_duration = (Instant::now() - test_start).as_secs_f64();
                let mut d = last_dura.lock().await;
                let this_dura = total_duration - *d;
                *d = total_duration;
                drop(d);

                let mut ws_results: Vec<WsApiResult> = Vec::with_capacity(ws_stats_list.len());
                for s in &ws_stats_list {
                    ws_results.push(snapshot_ws_api_result(s, total_duration).await);
                }

                // 每个 endpoint 的 rps
                for r in ws_results.iter_mut() {
                    let last = *api_last_sent.get(&r.name).unwrap_or(&0);
                    let delta = r.total_requests.saturating_sub(last);
                    api_last_sent.insert(r.name.clone(), r.total_requests);
                    let rps = if this_dura > 0.0 { delta as f64 / this_dura } else { 0.0 };
                    match api_rps_map.get_mut(&r.name) {
                        None => {
                            let q = fixed_size_queue::FixedSizeQueue::new(queue_cap);
                            api_rps_map.insert(r.name.clone(), q);
                            if let Some(q) = api_rps_map.get_mut(&r.name) { q.push(rps).await; }
                        }
                        Some(q) => q.push(rps).await,
                    }
                    r.rps = rps;
                }

                // 全局聚合
                let mut total_sent = 0u64;
                let mut total_recv = 0u64;
                let mut total_succ = 0u64;
                let mut total_err = 0i32;
                let mut total_bs = 0f64;
                let mut total_br = 0f64;
                let mut total_opened = 0u64;
                let mut total_failed = 0u64;
                let mut total_dropped = 0u64;
                let mut total_active = 0i32;
                let mut total_reconn = 0u64;
                let mut total_hbto = 0u64;
                let mut total_max = 0u64;
                let mut total_min = u64::MAX;
                let mut total_match_to = 0u64;
                for r in &ws_results {
                    total_sent += r.total_requests;
                    total_recv += r.messages_received;
                    total_succ += r.successful_requests;
                    total_err += r.err_count;
                    total_bs += r.bytes_sent_kb;
                    total_br += r.bytes_received_kb;
                    total_opened += r.connections_opened;
                    total_failed += r.connections_failed;
                    total_dropped += r.connections_dropped;
                    total_active += r.connections_active;
                    total_reconn += r.reconnects;
                    total_hbto += r.heartbeat_timeouts;
                    if r.max_response_time > total_max { total_max = r.max_response_time; }
                    // per-endpoint min 已把无数据哨兵归零, 这里跳过 0 只对有 RTT 的 endpoint 取最小
                    if r.min_response_time > 0 && r.min_response_time < total_min {
                        total_min = r.min_response_time;
                    }
                    total_match_to += r.match_timeouts;
                }
                if total_min == u64::MAX { total_min = 0; }

                // 分母用"已判定单元数" = 成功 + 错误, 与 per-endpoint 口径一致,
                // 避免 OneWay 模式下接收数 / 发送数 算出 >100% 的成功率
                let judged = total_succ + total_err.max(0) as u64;
                let success_rate = if judged > 0 { total_succ as f64 / judged as f64 * 100.0 } else { 0.0 };
                let error_rate = if judged > 0 { total_err as f64 / judged as f64 * 100.0 } else { 0.0 };

                let (hp50, hp95, hp99) = merged_percentiles(&ws_stats_list, true);
                let (rp50, rp95, rp99) = merged_percentiles(&ws_stats_list, false);

                let last_sent_v = last_total_sent.load(Ordering::SeqCst) as u64;
                let last_err_v = last_err.load(Ordering::SeqCst) as i32;
                let delta_sent = total_sent.saturating_sub(last_sent_v);
                let delta_err = (total_err - last_err_v).max(0);
                last_total_sent.store(total_sent as usize, Ordering::SeqCst);
                last_err.store(total_err as usize, Ordering::SeqCst);

                let rps = if this_dura > 0.0 { delta_sent as f64 / this_dura } else { 0.0 };
                {
                    let mut q = rps_queue.lock().await;
                    q.push(rps).await;
                }
                let throughput = if total_duration > 0.0 {
                    (total_bs + total_br) / total_duration
                } else { 0.0 };

                let avg_rt = if total_succ > 0 {
                    let sum_rt: u64 = ws_stats_list.iter()
                        .map(|s| s.total_response_time_ms.load(Ordering::Relaxed))
                        .sum();
                    (sum_rt as f64 / total_succ as f64).round() as u64
                } else { 0 };

                let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)
                    .map(|d| d.as_millis()).unwrap_or(0);

                // 为避免在 struct literal 表达式期间持有 parking_lot guard
                // (其 guard 不是 Send, 跨 await 后 spawn 时报 not-Send), 这里
                // 先把锁内 clone 出来再用.
                let ws_errors_snap = ws_errors.errors.lock().clone();
                let engine_errors_snap = {
                    let mut errs = engine_errors.lock();
                    let snap = errs.clone();
                    errs.clear();
                    snap
                };
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
                    errors_per_second: delta_err as usize,
                    rps,
                    total_data_sent_kb: total_bs,
                    total_data_received_kb: total_br,
                    throughput_per_second_kb: throughput,
                    median_response_time: rp50,
                    response_time_95: rp95,
                    response_time_99: rp99,
                    max_response_time: total_max,
                    min_response_time: total_min,
                    avg_response_time: avg_rt,
                    total_match_timeouts: total_match_to,
                    ws_results,
                    ws_errors: ws_errors_snap,
                    assert_errors: assert_errors_snap,
                    data_pool_stats: None,
                    engine_errors: engine_errors_snap,
                };
                if verbose {
                    println!("WS tick: total_sent={}, recv={}, active={}, rps={:.2}",
                        total_sent, total_recv, total_active, rps);
                }
                let _ = sender.send(Some(result)).await;
            }
        } => {}
    }
}
