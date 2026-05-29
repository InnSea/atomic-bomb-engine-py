use crate::models::api_endpoint::ThinkTime;
use crate::models::setup::SetupApiEndpoint;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// WebSocket 压测模式
///
/// - `OneWay`: 仅记录 messages_sent / messages_received, 不做请求-响应配对
/// - `RequestResponse`: 每条发送消息按 `match_by` 与回包配对, 统计 send→recv RTT
#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
pub enum WsMode {
    OneWay,
    RequestResponse {
        match_by: MessageMatcher,
        /// 等待回包超时, 0 表示无超时, 默认 30s
        #[serde(default = "default_match_timeout_ms")]
        timeout_ms: u64,
    },
}

fn default_match_timeout_ms() -> u64 {
    30_000
}

/// 请求-响应配对策略
#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
pub enum MessageMatcher {
    /// 按 JSON 字段配对, 发送时取 `send_path` 的值, 接收时取 `recv_path` 的值, 相等即配对成功.
    ///
    /// 同一配对键可有多条在途消息 (按 FIFO 排队, 回包弹出最早一条). 但要测出真实
    /// RTT 分布, `send_path` 的值应在在途消息间唯一 — 否则多条只能按到达顺序近似匹配.
    /// 模板里可用内置的 `{{ws_uuid}}` helper 生成唯一 id, 例如
    /// `{"id": "{{ws_uuid}}"}`, 无需依赖数据池。
    JsonPath {
        send_path: String,
        recv_path: String,
    },
    /// FIFO 顺序匹配: 接收到的第 N 条回包对应第 N 条仍在途的发送.
    ///
    /// 注意: 仅适用于"严格 1 发 1 收且服务端不主动推送"的场景.
    /// `on_connect` 消息不参与配对; 若服务端主动推送或对鉴权/订阅消息回包,
    /// 会让顺序错位。这类场景请改用 JsonPath 配对。
    Sequential,
}

/// WebSocket 消息载荷模板, 文本/JSON 字段会经过 handlebars 模板渲染
#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(tag = "type", content = "value")]
pub enum WsMessageTemplate {
    Text(String),
    Binary(Vec<u8>),
    Json(Value),
}

/// 周期性发送配置
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct SendPattern {
    /// 待发送的消息模板列表, 每个 tick 顺序轮转
    pub messages: Vec<WsMessageTemplate>,
    /// 每秒发送速率 (与 interval_ms 二选一, 同时存在以 rate_per_sec 为准)
    pub rate_per_sec: Option<f64>,
    /// 每两次发送的固定间隔毫秒数
    pub interval_ms: Option<u64>,
    /// 每次发送是否从数据池取一行写入 extract_map 后再渲染
    #[serde(default)]
    pub iterate_data_pool: bool,
}

/// 心跳配置
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Heartbeat {
    pub interval_secs: u64,
    /// 自定义心跳消息, None 表示使用 ws ping frame
    pub payload: Option<WsMessageTemplate>,
    /// 心跳超时时间, 超时未收到任何消息视为连接异常
    pub timeout_secs: u64,
}

/// 重连策略
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ReconnectPolicy {
    pub max_attempts: u32,
    pub backoff_ms: u64,
}

/// 断言触发时机
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub enum AssertTrigger {
    /// 每条接收消息都执行断言
    EveryMessage,
    /// 仅对第一条 jsonpath 命中的消息执行断言, 命中后该规则不再触发
    FirstMatch,
    /// 整个连接周期内都未匹配视为失败
    NoMatch,
}

/// WebSocket 断言选项
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct WsAssertOption {
    pub jsonpath: String,
    pub reference_object: Value,
    #[serde(default = "default_trigger")]
    pub on_match: AssertTrigger,
}

fn default_trigger() -> AssertTrigger {
    AssertTrigger::EveryMessage
}

/// 单个 WebSocket 接入点配置
///
/// 与 `ApiEndpoint` 平级, 但语义不同: 一个 WsEndpoint 对应一个或多个长连接,
/// 每个连接生命周期内可发送/接收多条消息.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct WsEndpoint {
    pub name: String,
    /// ws:// 或 wss:// URL, 支持 handlebars 模板
    pub url: String,
    /// 权重, 与 HTTP endpoint 同义, 决定该 endpoint 分到的连接数
    pub weight: u32,
    pub subprotocols: Option<Vec<String>>,
    pub headers: Option<HashMap<String, String>>,
    pub mode: WsMode,
    /// 连接建立后立即顺序发送的初始消息 (鉴权 / 订阅等)
    pub on_connect: Option<Vec<WsMessageTemplate>>,
    pub send_pattern: Option<SendPattern>,
    pub heartbeat: Option<Heartbeat>,
    /// 单连接最大持续时间, None 时跟随 batch 总时长
    pub connection_ttl_secs: Option<u64>,
    pub reconnect: Option<ReconnectPolicy>,
    pub assert_options: Option<Vec<WsAssertOption>>,
    pub think_time_option: Option<ThinkTime>,
    /// 复用 HTTP 的 setup 机制, 在 WS 连接建立前可发起 HTTP 请求做鉴权
    pub setup_options: Option<Vec<SetupApiEndpoint>>,
    pub teardown_options: Option<Vec<SetupApiEndpoint>>,
}
