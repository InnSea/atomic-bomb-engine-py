use crate::core::concurrency_controller::ConcurrencyController;
use crate::core::setup;
use crate::core::ws_endpoint_prebuilt::{EndpointPrebuilt, PrebuiltMatcher, PrebuiltMessage};
use crate::models::data_pool::DataPool;
use crate::models::ws_assert_task::WsAssertTask;
use crate::models::ws_endpoint::{
    Heartbeat, ReconnectPolicy, SendPattern, WsEndpoint, WsMode,
};
use crate::models::ws_endpoint_stats::WsEndpointStats;
use crate::models::ws_error_stats::{host_of, WsErrKind, WsErrorStats};
use anyhow::Error;
use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use handlebars::Handlebars;
use rand::Rng;
use reqwest::Client;
use serde_json::{json, Value};
use std::collections::{BTreeMap, VecDeque};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::handshake::client::Request;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

fn atomic_max(atomic: &AtomicU64, val: u64) {
    let mut current = atomic.load(Ordering::Relaxed);
    while val > current {
        match atomic.compare_exchange_weak(current, val, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(actual) => current = actual,
        }
    }
}

fn atomic_min(atomic: &AtomicU64, val: u64) {
    let mut current = atomic.load(Ordering::Relaxed);
    while val < current {
        match atomic.compare_exchange_weak(current, val, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(actual) => current = actual,
        }
    }
}


const SEQ_MATCH_KEY: &str = "\u{0}__ws_seq__";

const MAX_PENDING_PER_CONN: usize = 100_000;
fn render_prebuilt(
    msg: &PrebuiltMessage,
    hb: &Handlebars,
    ctx: &Value,
) -> (Message, usize, Option<Value>) {
    match msg {
        PrebuiltMessage::StaticText(t) => (
            Message::Text(t.clone()),
            t.len(),
            serde_json::from_str::<Value>(t).ok(),
        ),
        PrebuiltMessage::TemplatedText { name } => {
            let rendered = hb.render(name, ctx).unwrap_or_default();
            let bytes_len = rendered.len();
            let json_val = serde_json::from_str::<Value>(&rendered).ok();
            (Message::Text(rendered), bytes_len, json_val)
        }
        PrebuiltMessage::Binary(b) => (Message::Binary(b.clone()), b.len(), None),
        PrebuiltMessage::StaticJson { rendered, value } => (
            Message::Text(rendered.clone()),
            rendered.len(),
            Some(value.clone()),
        ),
        PrebuiltMessage::TemplatedJson { name, fallback } => {
            let rendered = hb.render(name, ctx).unwrap_or_else(|_| fallback.to_string());
            let bytes_len = rendered.len();
            let val = Value::from_str(&rendered).unwrap_or_else(|_| fallback.clone());
            (Message::Text(rendered), bytes_len, Some(val))
        }
    }
}

fn extract_send_id_prebuilt(
    matcher: &PrebuiltMatcher,
    json_val: &Option<Value>,
) -> Option<String> {
    match matcher {
        PrebuiltMatcher::JsonPath { send, .. } => {
            let val = json_val.as_ref()?;
            let res = send.select(val).ok()?;
            res.get(0).map(|v| value_to_match_key(v))
        }
        // Sequential: 所有消息进同一个 FIFO 队列
        PrebuiltMatcher::Sequential => Some(SEQ_MATCH_KEY.to_string()),
        PrebuiltMatcher::None => None,
    }
}

fn build_ws_request(
    url: &str,
    subprotocols: Option<&Vec<String>>,
    headers: Option<&std::collections::HashMap<String, String>>,
) -> Result<Request, Error> {
    let mut req = url
        .into_client_request()
        .map_err(|e| Error::msg(format!("构建 ws 请求失败: {:?}", e)))?;
    if let Some(protos) = subprotocols {
        if !protos.is_empty() {
            let val = protos.join(", ");
            req.headers_mut().insert(
                "Sec-WebSocket-Protocol",
                HeaderValue::from_str(&val)
                    .map_err(|e| Error::msg(format!("无效 subprotocol: {:?}", e)))?,
            );
        }
    }
    if let Some(headers_map) = headers {
        for (k, v) in headers_map {
            let name = tokio_tungstenite::tungstenite::http::HeaderName::from_bytes(k.as_bytes())
                .map_err(|e| Error::msg(format!("无效 header 名称 {}: {:?}", k, e)))?;
            let value = HeaderValue::from_str(v)
                .map_err(|e| Error::msg(format!("无效 header 值 {}: {:?}", v, e)))?;
            req.headers_mut().insert(name, value);
        }
    }
    Ok(req)
}


struct PendingState {
    map: DashMap<String, VecDeque<Instant>>,
    count: AtomicUsize,
}

impl PendingState {
    fn new() -> Self {
        Self {
            map: DashMap::new(),
            count: AtomicUsize::new(0),
        }
    }

    fn register(&self, key: String, now: Instant) -> bool {
        if self.count.load(Ordering::Relaxed) >= MAX_PENDING_PER_CONN {
            return false;
        }
        self.map.entry(key).or_default().push_back(now);
        self.count.fetch_add(1, Ordering::Relaxed);
        true
    }

    fn match_one(&self, key: &str) -> Option<Instant> {
        let mut entry = self.map.get_mut(key)?;
        let popped = entry.pop_front();
        if popped.is_some() {
            self.count.fetch_sub(1, Ordering::Relaxed);
        }
        popped
    }

    fn expire_older_than(&self, timeout: Duration, now: Instant) -> usize {
        let mut expired = 0usize;
        for mut entry in self.map.iter_mut() {
            while let Some(front) = entry.front() {
                if now.duration_since(*front) > timeout {
                    entry.pop_front();
                    expired += 1;
                } else {
                    break;
                }
            }
        }
        if expired > 0 {
            self.count.fetch_sub(expired, Ordering::Relaxed);
        }
        expired
    }
}

type PendingMap = Arc<PendingState>;

pub(crate) async fn start_ws_concurrency(
    controller: Arc<ConcurrencyController>,
    endpoint: Arc<WsEndpoint>,
    prebuilt: Arc<EndpointPrebuilt>,
    stats: Arc<WsEndpointStats>,
    extract_map: Arc<BTreeMap<String, Value>>,
    test_end: Instant,
    should_stop: Arc<std::sync::atomic::AtomicBool>,
    data_pool: Option<Arc<DataPool>>,
    tx_assert: Option<Sender<WsAssertTask>>,
    ws_errors: Arc<WsErrorStats>,
    assert_errors: crate::models::assert_error_stats::AssertErrorStats,
    engine_errors: Arc<parking_lot::Mutex<Vec<String>>>,
    http_client: Client,
    is_need_render_template: bool,
    verbose: bool,
) -> Result<(), Error> {
    let semaphore = controller.get_semaphore();
    let _permit = semaphore.acquire().await.expect("获取信号量许可失败");

    let handlebars = prebuilt.handlebars.clone();
    let endpoint_name = endpoint.name.clone();

    let mut reconnect_attempts: u32 = 0;

    'CONNECT: while Instant::now() < test_end && !should_stop.load(Ordering::SeqCst) {
        if let Some(tt) = &endpoint.think_time_option {
            if tt.min_millis <= tt.max_millis {
                let dur = rand::thread_rng().gen_range(tt.min_millis..=tt.max_millis);
                tokio::time::sleep(Duration::from_millis(dur)).await;
            }
        }

        let mut local_extract: BTreeMap<String, Value> = (*extract_map).clone();
        if let Some(pool) = &data_pool {
            for (key, value) in pool.get_next_row() {
                local_extract.insert(key, Value::String(value));
            }
        }
        if let Some(setup_options) = &endpoint.setup_options {
            match setup::start_setup(
                setup_options.clone(),
                local_extract.clone(),
                http_client.clone(),
            )
            .await
            {
                Ok(Some(extract)) => {
                    local_extract.extend(extract);
                }
                Ok(None) => {}
                Err(e) => {
                    let msg = format!("WS endpoint-{} setup 失败: {:?}", endpoint_name, e);
                    engine_errors.lock().push(msg);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue 'CONNECT;
                }
            }
        }

        // 3. 渲染 url
        let url = if is_need_render_template {
            handlebars
                .render_template(&endpoint.url, &json!(local_extract))
                .unwrap_or_else(|_| endpoint.url.clone())
        } else {
            endpoint.url.clone()
        };
        let host = host_of(&url);

        // 4. 渲染 headers
        let rendered_headers = endpoint.headers.as_ref().map(|hm| {
            hm.iter()
                .map(|(k, v)| {
                    let new_v = if is_need_render_template {
                        handlebars
                            .render_template(v, &json!(local_extract))
                            .unwrap_or_else(|_| v.clone())
                    } else {
                        v.clone()
                    };
                    (k.clone(), new_v)
                })
                .collect::<std::collections::HashMap<_, _>>()
        });

        // 5. 构建 ws 请求并握手
        let req = match build_ws_request(&url, endpoint.subprotocols.as_ref(), rendered_headers.as_ref()) {
            Ok(r) => r,
            Err(e) => {
                stats.connections_failed.fetch_add(1, Ordering::Relaxed);
                ws_errors.increment(
                    endpoint_name.clone(),
                    url.clone(),
                    host.clone(),
                    WsErrKind::Internal,
                    0,
                    e.to_string(),
                );
                if !should_reconnect(&endpoint.reconnect, &mut reconnect_attempts).await {
                    break 'CONNECT;
                }
                continue 'CONNECT;
            }
        };

        let connect_start = Instant::now();
        let ws_stream: WsStream = match connect_async(req).await {
            Ok((s, _resp)) => {
                stats.connections_opened.fetch_add(1, Ordering::Relaxed);
                stats.concurrent_number.fetch_add(1, Ordering::Relaxed);
                let elapsed_ms = connect_start.elapsed().as_millis() as u64;
                let _ = stats.handshake_histogram.increment(elapsed_ms);
                if verbose {
                    println!("WS-{} 握手成功 {}ms", endpoint_name, elapsed_ms);
                }
                s
            }
            Err(e) => {
                stats.connections_failed.fetch_add(1, Ordering::Relaxed);
                stats.err_count.fetch_add(1, Ordering::Relaxed);
                ws_errors.increment(
                    endpoint_name.clone(),
                    url.clone(),
                    host.clone(),
                    WsErrKind::Handshake,
                    0,
                    e.to_string(),
                );
                if !should_reconnect(&endpoint.reconnect, &mut reconnect_attempts).await {
                    break 'CONNECT;
                }
                stats.reconnects.fetch_add(1, Ordering::Relaxed);
                continue 'CONNECT;
            }
        };

        // 6. 进入连接生命周期
        let pending: PendingMap = Arc::new(PendingState::new());
        let last_recv = Arc::new(parking_lot::Mutex::new(Instant::now()));
        let conn_deadline = endpoint
            .connection_ttl_secs
            .filter(|&v| v > 0)
            .map(|v| Instant::now() + Duration::from_secs(v))
            .unwrap_or(test_end);
        let effective_deadline = std::cmp::min(conn_deadline, test_end);

        let exit_reason = run_connection_lifecycle(
            ws_stream,
            endpoint.clone(),
            prebuilt.clone(),
            stats.clone(),
            local_extract.clone(),
            pending.clone(),
            last_recv.clone(),
            effective_deadline,
            should_stop.clone(),
            data_pool.clone(),
            tx_assert.clone(),
            ws_errors.clone(),
            assert_errors.clone(),
            verbose,
            url.clone(),
        )
        .await;

        stats.concurrent_number.fetch_sub(1, Ordering::Relaxed);

        match exit_reason {
            ExitReason::Normal => {
                break 'CONNECT;
            }
            ExitReason::HeartbeatTimeout => {
                stats.heartbeat_timeouts.fetch_add(1, Ordering::Relaxed);
                stats.connections_dropped.fetch_add(1, Ordering::Relaxed);
                ws_errors.increment(
                    endpoint_name.clone(),
                    url.clone(),
                    host.clone(),
                    WsErrKind::HeartbeatTimeout,
                    0,
                    "心跳超时".to_string(),
                );
            }
            ExitReason::StreamError(msg) => {
                stats.connections_dropped.fetch_add(1, Ordering::Relaxed);
                ws_errors.increment(
                    endpoint_name.clone(),
                    url.clone(),
                    host.clone(),
                    WsErrKind::Stream,
                    0,
                    msg,
                );
            }
            ExitReason::ServerClose(code, msg) => {
                stats.connections_dropped.fetch_add(1, Ordering::Relaxed);
                ws_errors.increment(
                    endpoint_name.clone(),
                    url.clone(),
                    host.clone(),
                    WsErrKind::ServerClose,
                    code,
                    msg,
                );
            }
        }

        if !should_reconnect(&endpoint.reconnect, &mut reconnect_attempts).await {
            break 'CONNECT;
        }
        stats.reconnects.fetch_add(1, Ordering::Relaxed);
    }

    // teardown
    if let Some(teardown_opts) = &endpoint.teardown_options {
        let local_extract: BTreeMap<String, Value> = (*extract_map).clone();
        if let Err(e) = setup::start_setup(teardown_opts.clone(), local_extract, http_client.clone()).await {
            engine_errors
                .lock()
                .push(format!("WS endpoint-{} teardown 失败: {:?}", endpoint_name, e));
        }
    }
    Ok(())
}

#[derive(Debug)]
enum ExitReason {
    Normal,
    HeartbeatTimeout,
    StreamError(String),
    ServerClose(u16, String),
}

async fn should_reconnect(policy: &Option<ReconnectPolicy>, attempts: &mut u32) -> bool {
    match policy {
        Some(p) => {
            if *attempts >= p.max_attempts {
                return false;
            }
            *attempts += 1;
            tokio::time::sleep(Duration::from_millis(p.backoff_ms)).await;
            true
        }
        None => false,
    }
}

/// 连接生命周期主循环
async fn run_connection_lifecycle(
    ws: WsStream,
    endpoint: Arc<WsEndpoint>,
    prebuilt: Arc<EndpointPrebuilt>,
    stats: Arc<WsEndpointStats>,
    extract_map: BTreeMap<String, Value>,
    pending: PendingMap,
    last_recv: Arc<parking_lot::Mutex<Instant>>,
    deadline: Instant,
    should_stop: Arc<std::sync::atomic::AtomicBool>,
    data_pool: Option<Arc<DataPool>>,
    tx_assert: Option<Sender<WsAssertTask>>,
    ws_errors: Arc<WsErrorStats>,
    assert_errors: crate::models::assert_error_stats::AssertErrorStats,
    verbose: bool,
    url: String,
) -> ExitReason {
    let (write_half, read_half) = ws.split();
    let stream = read_half;
    let (write_tx, mut write_rx) = mpsc::channel::<Message>(64);
    let mut writer_task = tokio::spawn(async move {
        let mut sink = write_half;
        while let Some(m) = write_rx.recv().await {
            if sink.send(m).await.is_err() {
                break;
            }
        }
        let _ = sink.send(Message::Close(None)).await;
        let _ = sink.close().await;
    });

    if !prebuilt.on_connect.is_empty() {
        let ctx = json!(extract_map); // 渲染上下文序列化一次, 复用给所有 on_connect 消息
        for pmsg in &prebuilt.on_connect {
            let (msg, bytes_len, _) = render_prebuilt(pmsg, &prebuilt.handlebars, &ctx);
            if write_tx.send(msg).await.is_err() {
                return ExitReason::StreamError("on_connect 发送失败: writer 已退出".to_string());
            }
            stats.messages_sent.fetch_add(1, Ordering::Relaxed);
            stats.bytes_sent.fetch_add(bytes_len, Ordering::Relaxed);
        }
    }

    // 三个并发任务用 select! 汇合
    let send_task = sender_loop(
        write_tx.clone(),
        endpoint.clone(),
        prebuilt.clone(),
        stats.clone(),
        extract_map.clone(),
        pending.clone(),
        data_pool.clone(),
        deadline,
        should_stop.clone(),
        verbose,
    );
    let recv_task = receiver_loop(
        stream,
        endpoint.clone(),
        prebuilt.clone(),
        stats.clone(),
        pending.clone(),
        last_recv.clone(),
        tx_assert.clone(),
        ws_errors.clone(),
        assert_errors.clone(),
        verbose,
        url.clone(),
    );
    let hb_task = heartbeat_loop(
        write_tx.clone(),
        prebuilt.clone(),
        stats.clone(),
        endpoint.heartbeat.clone(),
        last_recv.clone(),
        extract_map.clone(),
    );
    let timeout_task = async {
        tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
    };
    let stop_task = async {
        loop {
            if should_stop.load(Ordering::SeqCst) {
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    };
    let match_timeout_task = match_timeout_watcher(
        endpoint.clone(),
        stats.clone(),
        pending.clone(),
        deadline,
    );

    let exit = tokio::select! {
        r = send_task => r,
        r = recv_task => r,
        r = hb_task => r,
        _ = timeout_task => ExitReason::Normal,
        _ = stop_task => ExitReason::Normal,
        _ = match_timeout_task => ExitReason::Normal,
    };
    // 优雅关闭: drop write_tx 让 writer_task 收到 channel 关闭信号, 它会自己
    // 发 Close frame 并关流. 加 2s 超时 — 对端不 ACK Close frame 时
    // sink.close() 会无限挂住, 几千连接同时关闭场景下能拖死整个 batch teardown.
    // 超时后 abort 强制释放 (TCP RST), 个别连接关闭语义不优雅但无副作用.
    drop(write_tx);
    if tokio::time::timeout(Duration::from_secs(2), &mut writer_task)
        .await
        .is_err()
    {
        writer_task.abort();
    }

    exit
}

async fn sender_loop(
    write_tx: mpsc::Sender<Message>,
    endpoint: Arc<WsEndpoint>,
    prebuilt: Arc<EndpointPrebuilt>,
    stats: Arc<WsEndpointStats>,
    base_extract: BTreeMap<String, Value>,
    pending: PendingMap,
    data_pool: Option<Arc<DataPool>>,
    deadline: Instant,
    should_stop: Arc<std::sync::atomic::AtomicBool>,
    verbose: bool,
) -> ExitReason {
    let pattern = match &endpoint.send_pattern {
        Some(p) => p.clone(),
        None => {
            return wait_forever(deadline, should_stop).await;
        }
    };

    let interval_ms = compute_interval_ms(&pattern).max(1);
    let mut idx = 0usize;
    let handlebars = prebuilt.handlebars.clone();
    let mut ticker = tokio::time::interval(Duration::from_millis(interval_ms));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let static_ctx = if pattern.iterate_data_pool {
        None
    } else {
        Some(json!(base_extract))
    };

    if prebuilt.send_messages.is_empty() {
        return wait_forever(deadline, should_stop).await;
    }

    loop {
        ticker.tick().await;
        if Instant::now() >= deadline || should_stop.load(Ordering::SeqCst) {
            return ExitReason::Normal;
        }

        let ctx_owned;
        let ctx: &Value = if let Some(ref c) = static_ctx {
            c
        } else {
            let mut local_extract = base_extract.clone();
            if let Some(pool) = &data_pool {
                for (k, v) in pool.get_next_row() {
                    local_extract.insert(k, Value::String(v));
                }
            }
            ctx_owned = json!(local_extract);
            &ctx_owned
        };

        let pmsg = &prebuilt.send_messages[idx % prebuilt.send_messages.len()];
        idx = idx.wrapping_add(1);

        let (msg, bytes_len, json_val) = render_prebuilt(pmsg, &handlebars, ctx);

        if matches!(endpoint.mode, WsMode::RequestResponse { .. }) {
            if let Some(send_id) = extract_send_id_prebuilt(&prebuilt.matcher, &json_val) {
                if !pending.register(send_id, Instant::now()) {
                    stats.match_timeouts.fetch_add(1, Ordering::Relaxed);
                    stats.err_count.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        if write_tx.send(msg).await.is_err() {
            return ExitReason::StreamError("发送失败: writer 已退出".to_string());
        }
        stats.messages_sent.fetch_add(1, Ordering::Relaxed);
        stats.bytes_sent.fetch_add(bytes_len, Ordering::Relaxed);
        if verbose {
            println!("WS-{} 发送 {} bytes", endpoint.name, bytes_len);
        }
    }
}

async fn wait_forever(
    deadline: Instant,
    should_stop: Arc<std::sync::atomic::AtomicBool>,
) -> ExitReason {
    loop {
        if Instant::now() >= deadline || should_stop.load(Ordering::SeqCst) {
            return ExitReason::Normal;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

fn compute_interval_ms(pattern: &SendPattern) -> u64 {
    if let Some(rate) = pattern.rate_per_sec {
        if rate > 0.0 {
            return (1000.0 / rate).max(1.0) as u64;
        }
    }
    pattern.interval_ms.unwrap_or(1000)
}


fn value_to_match_key(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        other => other.to_string(),
    }
}

async fn receiver_loop(
    mut stream: futures_util::stream::SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    endpoint: Arc<WsEndpoint>,
    prebuilt: Arc<EndpointPrebuilt>,
    stats: Arc<WsEndpointStats>,
    pending: PendingMap,
    last_recv: Arc<parking_lot::Mutex<Instant>>,
    tx_assert: Option<Sender<WsAssertTask>>,
    _ws_errors: Arc<WsErrorStats>,
    assert_errors: crate::models::assert_error_stats::AssertErrorStats,
    verbose: bool,
    url: String,
) -> ExitReason {
    loop {
        let next = stream.next().await;
        match next {
            None => {
                return ExitReason::ServerClose(0, "对端关闭(无 frame)".to_string());
            }
            Some(Err(e)) => {
                return ExitReason::StreamError(format!("接收失败: {:?}", e));
            }
            Some(Ok(msg)) => {
                *last_recv.lock() = Instant::now();
                match msg {
                    Message::Text(t) => {
                        on_message_received(
                            t.as_bytes(),
                            true,
                            &endpoint,
                            &prebuilt,
                            &stats,
                            &pending,
                            &tx_assert,
                            &assert_errors,
                            verbose,
                        )
                        .await;
                    }
                    Message::Binary(b) => {
                        on_message_received(
                            &b,
                            false,
                            &endpoint,
                            &prebuilt,
                            &stats,
                            &pending,
                            &tx_assert,
                            &assert_errors,
                            verbose,
                        )
                        .await;
                    }
                    Message::Ping(_) => {
                        // tungstenite 默认会自动回 Pong, 这里不重复处理
                    }
                    Message::Pong(_) => {
                        // 心跳响应已在 last_recv 更新
                    }
                    Message::Close(frame) => {
                        let (code, reason) = match frame {
                            Some(f) => (close_code_to_u16(f.code), f.reason.to_string()),
                            None => (0, "对端发送 close frame".to_string()),
                        };
                        let _ = url; // 保留, 用于将来日志
                        return ExitReason::ServerClose(code, reason);
                    }
                    Message::Frame(_) => {}
                }
            }
        }
    }
}

fn close_code_to_u16(code: CloseCode) -> u16 {
    code.into()
}

async fn on_message_received(
    payload: &[u8],
    is_text: bool,
    endpoint: &Arc<WsEndpoint>,
    prebuilt: &Arc<EndpointPrebuilt>,
    stats: &Arc<WsEndpointStats>,
    pending: &PendingMap,
    tx_assert: &Option<Sender<WsAssertTask>>,
    assert_errors: &crate::models::assert_error_stats::AssertErrorStats,
    verbose: bool,
) {
    stats.messages_received.fetch_add(1, Ordering::Relaxed);
    stats.bytes_received.fetch_add(payload.len(), Ordering::Relaxed);

    let mut matched_rtt_ms: Option<u64> = None;
    if matches!(endpoint.mode, WsMode::RequestResponse { .. }) {
        let recv_id = match &prebuilt.matcher {
            PrebuiltMatcher::JsonPath { recv, .. } => {
                if !is_text {
                    None
                } else {
                    serde_json::from_slice::<Value>(payload)
                        .ok()
                        .and_then(|v| recv.select(&v).ok().and_then(|res| res.get(0).map(|x| value_to_match_key(x))))
                }
            }
            PrebuiltMatcher::Sequential => Some(SEQ_MATCH_KEY.to_string()),
            PrebuiltMatcher::None => None,
        };
        if let Some(rid) = recv_id {
            if let Some(sent_at) = pending.match_one(&rid) {
                let rtt_ms = sent_at.elapsed().as_millis() as u64;
                let _ = stats.rtt_histogram.increment(rtt_ms);
                stats.total_response_time_ms.fetch_add(rtt_ms, Ordering::Relaxed);
                atomic_max(&stats.max_response_time, rtt_ms);
                atomic_min(&stats.min_response_time, rtt_ms);
                stats.successful_requests.fetch_add(1, Ordering::Relaxed);
                matched_rtt_ms = Some(rtt_ms);
            }
        }
    } else {
        stats.successful_requests.fetch_add(1, Ordering::Relaxed);
    }

    if verbose {
        if let Some(rtt) = matched_rtt_ms {
            println!("WS-{} 配对成功 RTT={}ms", endpoint.name, rtt);
        }
    }

    // 断言投递
    if is_text {
        if let (Some(tx), Some(opts)) = (tx_assert.as_ref(), endpoint.assert_options.as_ref()) {
            if !opts.is_empty() {
                let task = WsAssertTask {
                    endpoint_name: endpoint.name.clone(),
                    endpoint_url: endpoint.url.clone(),
                    assert_options: opts.clone(),
                    body_bytes: payload.to_vec(),
                    verbose,
                    ws_stats: stats.clone(),
                    assert_errors: assert_errors.clone(),
                };
                let _ = tx.send(task).await;
            }
        }
    }
}

async fn heartbeat_loop(
    write_tx: mpsc::Sender<Message>,
    prebuilt: Arc<EndpointPrebuilt>,
    stats: Arc<WsEndpointStats>,
    heartbeat: Option<Heartbeat>,
    last_recv: Arc<parking_lot::Mutex<Instant>>,
    extract_map: BTreeMap<String, Value>,
) -> ExitReason {
    let hb = match heartbeat {
        Some(h) => h,
        None => return std::future::pending::<ExitReason>().await,
    };
    let interval = Duration::from_secs(hb.interval_secs.max(1));
    let timeout = Duration::from_secs(hb.timeout_secs.max(1));
    let handlebars = prebuilt.handlebars.clone();
    let ctx = json!(extract_map);

    let mut ticker = tokio::time::interval(interval);
    ticker.tick().await;

    loop {
        ticker.tick().await;

        // 检查超时
        let last = *last_recv.lock();
        if last.elapsed() > timeout {
            return ExitReason::HeartbeatTimeout;
        }

        let (msg, bytes_len) = match &prebuilt.heartbeat_payload {
            None => (Message::Ping(Vec::new()), 0),
            Some(pmsg) => {
                let (m, len, _) = render_prebuilt(pmsg, &handlebars, &ctx);
                (m, len)
            }
        };
        if write_tx.send(msg).await.is_err() {
            return ExitReason::StreamError("心跳发送失败: writer 已退出".to_string());
        }
        stats.bytes_sent.fetch_add(bytes_len, Ordering::Relaxed);
    }
}

async fn match_timeout_watcher(
    endpoint: Arc<WsEndpoint>,
    stats: Arc<WsEndpointStats>,
    pending: PendingMap,
    deadline: Instant,
) -> ExitReason {
    let timeout_ms = match &endpoint.mode {
        WsMode::RequestResponse { timeout_ms, .. } => *timeout_ms,
        _ => return std::future::pending::<ExitReason>().await,
    };
    if timeout_ms == 0 {
        return std::future::pending::<ExitReason>().await;
    }
    let timeout = Duration::from_millis(timeout_ms);
    let mut ticker = tokio::time::interval(Duration::from_millis(500));
    loop {
        ticker.tick().await;
        if Instant::now() >= deadline {
            return ExitReason::Normal;
        }
        let expired = pending.expire_older_than(timeout, Instant::now());
        if expired > 0 {
            stats.match_timeouts.fetch_add(expired, Ordering::Relaxed);
            stats.err_count.fetch_add(expired, Ordering::Relaxed);
        }
    }
}
