use histogram::AtomicHistogram;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::sync::Arc;

/// 每个接口的运行时原子统计容器
/// 热路径纯原子操作: 计数器 + AtomicHistogram (内部用 AtomicU64 bucket, 无锁 increment)
/// ApiResult 快照在 share_result / batch 结束时由原子值组装, histogram 通过 load() 取 snapshot 读 percentile
pub struct ApiEndpointStats {
    pub name: String,
    pub url: String,
    pub method: String,
    pub concurrent_number: Arc<AtomicUsize>,
    pub total_requests: Arc<AtomicUsize>,
    pub successful_requests: Arc<AtomicUsize>,
    pub err_count: Arc<AtomicUsize>,
    pub total_response_time_ms: Arc<AtomicU64>,
    pub total_response_size: Arc<AtomicUsize>,
    pub max_response_time: Arc<AtomicU64>,
    pub min_response_time: Arc<AtomicU64>,
    pub histogram: Arc<AtomicHistogram>,
}

impl ApiEndpointStats {
    pub fn new(
        name: String,
        url: String,
        method: String,
        histogram: Arc<AtomicHistogram>,
    ) -> Self {
        Self {
            name,
            url,
            method,
            concurrent_number: Arc::new(AtomicUsize::new(0)),
            total_requests: Arc::new(AtomicUsize::new(0)),
            successful_requests: Arc::new(AtomicUsize::new(0)),
            err_count: Arc::new(AtomicUsize::new(0)),
            total_response_time_ms: Arc::new(AtomicU64::new(0)),
            total_response_size: Arc::new(AtomicUsize::new(0)),
            max_response_time: Arc::new(AtomicU64::new(0)),
            min_response_time: Arc::new(AtomicU64::new(u64::MAX)),
            histogram,
        }
    }
}
