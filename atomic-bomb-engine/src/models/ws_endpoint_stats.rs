use histogram::AtomicHistogram;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::sync::Arc;

/// 单个 WS endpoint 的运行时统计容器
///
/// 设计与 `ApiEndpointStats` 一致: 热路径全部走原子操作, 直方图用 AtomicHistogram 无锁 increment.
/// 但语义不同: WS 关注的是连接生命周期 + 消息流, 所以新增连接级和消息级计数.
pub struct WsEndpointStats {
    pub name: String,
    pub url: String,

    // 连接级
    /// 当前活跃连接数 (open - close)
    pub concurrent_number: Arc<AtomicUsize>,
    /// 累计成功握手次数
    pub connections_opened: Arc<AtomicUsize>,
    /// 握手失败次数
    pub connections_failed: Arc<AtomicUsize>,
    /// 异常断开次数 (包含心跳超时, 协议错误)
    pub connections_dropped: Arc<AtomicUsize>,
    /// 心跳超时次数
    pub heartbeat_timeouts: Arc<AtomicUsize>,
    /// 重连次数
    pub reconnects: Arc<AtomicUsize>,
    /// 握手延迟直方图 (单位 ms)
    pub handshake_histogram: Arc<AtomicHistogram>,

    // 消息级
    /// 已发送消息数 (作为 total_requests 在结果中暴露, 与 HTTP 语义对齐)
    pub messages_sent: Arc<AtomicUsize>,
    /// 已接收消息数
    pub messages_received: Arc<AtomicUsize>,
    /// 成功消息数:
    /// - OneWay 模式 = 发送成功的消息数
    /// - RequestResponse 模式 = 配对成功的回包数
    pub successful_requests: Arc<AtomicUsize>,
    /// 业务错误总数
    pub err_count: Arc<AtomicUsize>,
    /// 累计发送字节数
    pub bytes_sent: Arc<AtomicUsize>,
    /// 累计接收字节数
    pub bytes_received: Arc<AtomicUsize>,
    /// RequestResponse 模式: 配对超时未回的消息数
    pub match_timeouts: Arc<AtomicUsize>,
    /// RequestResponse 模式 RTT 直方图 (单位 ms)
    pub rtt_histogram: Arc<AtomicHistogram>,
    /// 单条消息收发延迟最大值 (ms, RTT 或单向)
    pub max_response_time: Arc<AtomicU64>,
    /// 单条消息收发延迟最小值 (ms)
    pub min_response_time: Arc<AtomicU64>,
    /// 累计响应时间 (ms), 用于均值
    pub total_response_time_ms: Arc<AtomicU64>,
}

impl WsEndpointStats {
    pub fn new(
        name: String,
        url: String,
        handshake_histogram: Arc<AtomicHistogram>,
        rtt_histogram: Arc<AtomicHistogram>,
    ) -> Self {
        Self {
            name,
            url,
            concurrent_number: Arc::new(AtomicUsize::new(0)),
            connections_opened: Arc::new(AtomicUsize::new(0)),
            connections_failed: Arc::new(AtomicUsize::new(0)),
            connections_dropped: Arc::new(AtomicUsize::new(0)),
            heartbeat_timeouts: Arc::new(AtomicUsize::new(0)),
            reconnects: Arc::new(AtomicUsize::new(0)),
            handshake_histogram,
            messages_sent: Arc::new(AtomicUsize::new(0)),
            messages_received: Arc::new(AtomicUsize::new(0)),
            successful_requests: Arc::new(AtomicUsize::new(0)),
            err_count: Arc::new(AtomicUsize::new(0)),
            bytes_sent: Arc::new(AtomicUsize::new(0)),
            bytes_received: Arc::new(AtomicUsize::new(0)),
            match_timeouts: Arc::new(AtomicUsize::new(0)),
            rtt_histogram,
            max_response_time: Arc::new(AtomicU64::new(0)),
            min_response_time: Arc::new(AtomicU64::new(u64::MAX)),
            total_response_time_ms: Arc::new(AtomicU64::new(0)),
        }
    }
}
