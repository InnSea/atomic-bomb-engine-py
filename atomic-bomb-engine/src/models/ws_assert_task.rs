use crate::models::assert_error_stats::AssertErrorStats;
use crate::models::ws_endpoint::WsAssertOption;
use crate::models::ws_endpoint_stats::WsEndpointStats;
use std::sync::Arc;

/// WS 断言任务: 接收侧每收到一条文本/JSON 消息时投递, worker 侧执行 jsonpath 校验
pub struct WsAssertTask {
    pub(crate) endpoint_name: String,
    pub(crate) endpoint_url: String,
    pub(crate) assert_options: Vec<WsAssertOption>,
    pub(crate) body_bytes: Vec<u8>,
    pub(crate) verbose: bool,
    pub(crate) ws_stats: Arc<WsEndpointStats>,
    /// AssertErrorStats 内部已经是 Arc<parking_lot::Mutex<HashMap>>, 自身就是
    /// 共享句柄, 不需要再套一层 Arc<tokio::Mutex<>>. 直接 clone 即可共享.
    pub(crate) assert_errors: AssertErrorStats,
}
