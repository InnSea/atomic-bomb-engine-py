use crate::models::assert_error_stats::AssertErrKey;
use crate::models::data_pool::DataPoolStats;
use crate::models::ws_error_stats::WsErrKey;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// 单个 WS endpoint 的快照结果, 与 HTTP 的 `ApiResult` 平级但字段不同
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsApiResult {
    pub name: String,
    pub url: String,
    pub host: String,
    pub path: String,

    // 连接级
    pub connections_opened: u64,
    pub connections_active: i32,
    pub connections_failed: u64,
    pub connections_dropped: u64,
    pub heartbeat_timeouts: u64,
    pub reconnects: u64,

    // 握手延迟分位
    pub handshake_p50_ms: u64,
    pub handshake_p95_ms: u64,
    pub handshake_p99_ms: u64,

    // 消息级 (与 HTTP 的 ApiResult 字段尽量保持同名以便复用前端展示)
    pub total_requests: u64, // = messages_sent
    pub messages_received: u64,
    pub successful_requests: u64,
    pub success_rate: f64,
    pub error_rate: f64,
    pub err_count: i32,
    pub rps: f64, // = messages_sent / duration

    // 字节
    pub bytes_sent_kb: f64,
    pub bytes_received_kb: f64,
    pub throughput_per_second_kb: f64,

    // RTT (RequestResponse 模式有效, 其他模式 0)
    pub median_response_time: u64,
    pub response_time_95: u64,
    pub response_time_99: u64,
    pub max_response_time: u64,
    pub min_response_time: u64,
    pub avg_response_time: u64,
    pub match_timeouts: u64,
}

impl WsApiResult {
    pub fn new() -> Self {
        Self {
            name: String::new(),
            url: String::new(),
            host: String::new(),
            path: String::new(),
            connections_opened: 0,
            connections_active: 0,
            connections_failed: 0,
            connections_dropped: 0,
            heartbeat_timeouts: 0,
            reconnects: 0,
            handshake_p50_ms: 0,
            handshake_p95_ms: 0,
            handshake_p99_ms: 0,
            total_requests: 0,
            messages_received: 0,
            successful_requests: 0,
            success_rate: 0.0,
            error_rate: 0.0,
            err_count: 0,
            rps: 0.0,
            bytes_sent_kb: 0.0,
            bytes_received_kb: 0.0,
            throughput_per_second_kb: 0.0,
            median_response_time: 0,
            response_time_95: 0,
            response_time_99: 0,
            max_response_time: 0,
            min_response_time: 0,
            avg_response_time: 0,
            match_timeouts: 0,
        }
    }
}

/// WS 压测整体结果, 与 HTTP 的 `BatchResult` 完全独立
///
/// 之所以不复用 BatchResult, 是为了:
/// 1. 不破坏 BatchResult 已有的字段语义和 Python dict 结构
/// 2. WS 关心的指标 (连接级 / 消息级) 与 HTTP 不同, 强行混用会让用户困惑
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsBatchResult {
    pub total_duration: f64,
    pub timestamp: u128,

    // 连接级汇总
    pub total_connections_opened: u64,
    pub total_connections_failed: u64,
    pub total_connections_dropped: u64,
    pub total_concurrent_number: i32,
    pub total_reconnects: u64,
    pub total_heartbeat_timeouts: u64,

    // 握手分位
    pub handshake_p50_ms: u64,
    pub handshake_p95_ms: u64,
    pub handshake_p99_ms: u64,

    // 消息级汇总
    pub total_messages_sent: u64,
    pub total_messages_received: u64,
    pub successful_requests: u64,
    pub success_rate: f64,
    pub error_rate: f64,
    pub err_count: i32,
    pub errors_per_second: usize,
    pub rps: f64,

    // 字节
    pub total_data_sent_kb: f64,
    pub total_data_received_kb: f64,
    pub throughput_per_second_kb: f64,

    // RTT 分位 (RequestResponse 模式)
    pub median_response_time: u64,
    pub response_time_95: u64,
    pub response_time_99: u64,
    pub max_response_time: u64,
    pub min_response_time: u64,
    pub avg_response_time: u64,
    pub total_match_timeouts: u64,

    // 明细
    pub ws_results: Vec<WsApiResult>,
    pub ws_errors: HashMap<WsErrKey, u32>,
    pub assert_errors: HashMap<AssertErrKey, u32>,
    pub data_pool_stats: Option<DataPoolStats>,
    pub engine_errors: Vec<String>,
}

impl WsBatchResult {
    pub fn empty_with_engine_errors(errors: Vec<String>) -> Self {
        Self {
            total_duration: 0.0,
            timestamp: 0,
            total_connections_opened: 0,
            total_connections_failed: 0,
            total_connections_dropped: 0,
            total_concurrent_number: 0,
            total_reconnects: 0,
            total_heartbeat_timeouts: 0,
            handshake_p50_ms: 0,
            handshake_p95_ms: 0,
            handshake_p99_ms: 0,
            total_messages_sent: 0,
            total_messages_received: 0,
            successful_requests: 0,
            success_rate: 0.0,
            error_rate: 0.0,
            err_count: 0,
            errors_per_second: 0,
            rps: 0.0,
            total_data_sent_kb: 0.0,
            total_data_received_kb: 0.0,
            throughput_per_second_kb: 0.0,
            median_response_time: 0,
            response_time_95: 0,
            response_time_99: 0,
            max_response_time: 0,
            min_response_time: 0,
            avg_response_time: 0,
            total_match_timeouts: 0,
            ws_results: Vec::new(),
            ws_errors: HashMap::new(),
            assert_errors: HashMap::new(),
            data_pool_stats: None,
            engine_errors: errors,
        }
    }
}
