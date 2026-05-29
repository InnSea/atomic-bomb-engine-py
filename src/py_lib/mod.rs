pub(crate) mod assert_option_func;
pub(crate) mod batch_runner;
pub(crate) mod data_pool_func;
pub(crate) mod endpoint_func;
pub(crate) mod jsonpath_extract_func;
pub(crate) mod multipart_option_func;
pub(crate) mod setup_option_func;
pub(crate) mod step_option_func;
pub(crate) mod think_time_option_func;

// ========== WebSocket 模块 (独立, 不影响 HTTP 路径) ==========
pub(crate) mod ws_batch_runner;
pub(crate) mod ws_endpoint_func;
pub(crate) mod ws_match_func;
pub(crate) mod ws_message_func;
pub(crate) mod ws_options_func;