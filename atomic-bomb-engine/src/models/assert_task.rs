use crate::models::api_endpoint::ApiEndpoint;
use crate::models::assert_error_stats::AssertErrorStats;
use crate::models::assert_option::AssertOption;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

#[derive(Debug)]
pub struct AssertTask {
    pub(crate) assert_options: Vec<AssertOption>,
    pub(crate) body_bytes: Vec<u8>,
    pub(crate) verbose: bool,
    pub(crate) err_count: Arc<AtomicUsize>,
    pub(crate) api_err_count: Arc<AtomicUsize>,
    /// AssertErrorStats 内部已经是 Arc<parking_lot::Mutex<HashMap>>, 自身就是
    /// 共享句柄, 不需要再套一层 Arc<tokio::Mutex<>>. 直接 clone 即可共享.
    pub(crate) assert_errors: AssertErrorStats,
    pub(crate) endpoint: Arc<ApiEndpoint>,
    pub(crate) api_name: String,
    pub(crate) successful_requests: Arc<AtomicUsize>,
    pub(crate) api_successful_requests: Arc<AtomicUsize>,
}
