//! WebSocket endpoint 启动前的一次性预编译.
//!
//! 一个 endpoint 在压测期间会被 `concurrent_connections` 条连接 + 每条连接
//! N 次发送 + M 次回包反复使用. send_pattern 的消息模板、match_by 的
//! jsonpath 在 endpoint 维度是常量, 但 hot path 上每条消息都要 parse 一次:
//!
//! - `Handlebars::render_template(t, ctx)` 内部每次 parse 模板字符串
//! - `jsonpath_lib::select(v, path)` 内部每次 parse 路径字符串
//!
//! 3000 连接 × 6000 msg/s 量级下这是 ~10k 次/s 的纯浪费. 本模块在 ws_batch
//! 启动阶段把这些常量字符串编译成 [`Compiled`] / 注册成命名 handlebars 模板,
//! hot path 直接复用, 把 parse 成本摊平到一次性.

use crate::models::ws_endpoint::{
    Heartbeat, MessageMatcher, SendPattern, WsEndpoint, WsMessageTemplate, WsMode,
};
use anyhow::Error;
use handlebars::Handlebars;
use jsonpath_lib::Compiled;
use serde_json::Value;
use std::sync::Arc;

/// 单条预编译消息模板的形态.
///
/// `Static` 表示模板里没有 `{{...}}`, 渲染结果就是字符串/字节本身,
/// 完全跳过 handlebars; `Templated` 才走 handlebars 渲染.
pub enum PrebuiltMessage {
    /// 文本静态消息: 已渲染的字符串
    StaticText(String),
    /// 文本模板: 在 handlebars 里以 [`Self::name`] 注册, 用 `hb.render(name, ctx)` 渲染
    TemplatedText { name: String },
    /// 二进制 (二进制消息没有模板能力)
    Binary(Vec<u8>),
    /// JSON 静态消息: 已序列化的字符串 + 解析好的 Value
    StaticJson { rendered: String, value: Value },
    /// JSON 模板: handlebars 命名模板
    TemplatedJson { name: String, fallback: Value },
}

impl PrebuiltMessage {
    fn from_template(
        idx: usize,
        scope: &str,
        template: &WsMessageTemplate,
        hb: &mut Handlebars<'static>,
    ) -> Result<Self, Error> {
        match template {
            WsMessageTemplate::Text(t) => {
                if has_handlebars_var(t) {
                    let name = format!("{}.text.{}", scope, idx);
                    hb.register_template_string(&name, t)
                        .map_err(|e| Error::msg(format!("注册文本模板 {} 失败: {:?}", name, e)))?;
                    Ok(PrebuiltMessage::TemplatedText { name })
                } else {
                    Ok(PrebuiltMessage::StaticText(t.clone()))
                }
            }
            WsMessageTemplate::Binary(b) => Ok(PrebuiltMessage::Binary(b.clone())),
            WsMessageTemplate::Json(v) => {
                let raw = v.to_string();
                if has_handlebars_var(&raw) {
                    let name = format!("{}.json.{}", scope, idx);
                    hb.register_template_string(&name, &raw)
                        .map_err(|e| Error::msg(format!("注册 JSON 模板 {} 失败: {:?}", name, e)))?;
                    Ok(PrebuiltMessage::TemplatedJson {
                        name,
                        fallback: v.clone(),
                    })
                } else {
                    Ok(PrebuiltMessage::StaticJson {
                        rendered: raw,
                        value: v.clone(),
                    })
                }
            }
        }
    }
}

/// 粗略判断模板里有没有 `{{...}}` 占位符. 不需要严格的 handlebars 语法分析,
/// 误判成 templated 顶多多走一次 render, 不会出错; 误判成 static 才有问题
/// (会把 `{{foo}}` 原样发出). 用最保守的存在性判定即可.
fn has_handlebars_var(s: &str) -> bool {
    s.contains("{{")
}

/// match_by 的预编译. JsonPath 模式下把 send_path 和 recv_path 都编译成
/// [`Compiled`]; Sequential 模式什么都不需要.
pub enum PrebuiltMatcher {
    None,
    JsonPath {
        send: Compiled,
        recv: Compiled,
    },
    Sequential,
}

/// endpoint 维度的预编译产物. 由 ws_batch 启动阶段构建一次, 通过 Arc 共享给
/// 所有连接, hot path 只读, 无锁.
pub struct EndpointPrebuilt {
    pub send_messages: Vec<PrebuiltMessage>,
    pub on_connect: Vec<PrebuiltMessage>,
    pub heartbeat_payload: Option<PrebuiltMessage>,
    pub matcher: PrebuiltMatcher,
    /// 共享的 handlebars 实例, 已注册所有命名模板和 ws_uuid helper
    pub handlebars: Arc<Handlebars<'static>>,
}

impl EndpointPrebuilt {
    pub fn build(endpoint: &WsEndpoint) -> Result<Self, Error> {
        let mut hb = build_handlebars();
        let scope = format!("ep.{}", endpoint.name);

        let send_messages = match &endpoint.send_pattern {
            Some(SendPattern { messages, .. }) => messages
                .iter()
                .enumerate()
                .map(|(i, m)| PrebuiltMessage::from_template(i, &format!("{}.send", scope), m, &mut hb))
                .collect::<Result<Vec<_>, _>>()?,
            None => Vec::new(),
        };
        let on_connect = match &endpoint.on_connect {
            Some(msgs) => msgs
                .iter()
                .enumerate()
                .map(|(i, m)| PrebuiltMessage::from_template(i, &format!("{}.onconn", scope), m, &mut hb))
                .collect::<Result<Vec<_>, _>>()?,
            None => Vec::new(),
        };
        let heartbeat_payload = match &endpoint.heartbeat {
            Some(Heartbeat { payload: Some(tpl), .. }) => {
                Some(PrebuiltMessage::from_template(0, &format!("{}.hb", scope), tpl, &mut hb)?)
            }
            _ => None,
        };
        let matcher = match &endpoint.mode {
            WsMode::OneWay => PrebuiltMatcher::None,
            WsMode::RequestResponse {
                match_by: MessageMatcher::JsonPath { send_path, recv_path },
                ..
            } => {
                let send = Compiled::compile(send_path)
                    .map_err(|e| Error::msg(format!("send_path 编译失败 {}: {:?}", send_path, e)))?;
                let recv = Compiled::compile(recv_path)
                    .map_err(|e| Error::msg(format!("recv_path 编译失败 {}: {:?}", recv_path, e)))?;
                PrebuiltMatcher::JsonPath { send, recv }
            }
            WsMode::RequestResponse {
                match_by: MessageMatcher::Sequential,
                ..
            } => PrebuiltMatcher::Sequential,
        };

        Ok(EndpointPrebuilt {
            send_messages,
            on_connect,
            heartbeat_payload,
            matcher,
            handlebars: Arc::new(hb),
        })
    }
}

/// `{{ws_uuid}}` 模板 helper: 每次渲染生成一个 v4 UUID.
fn ws_uuid_helper(
    _: &handlebars::Helper,
    _: &Handlebars,
    _: &handlebars::Context,
    _: &mut handlebars::RenderContext,
    out: &mut dyn handlebars::Output,
) -> handlebars::HelperResult {
    out.write(&uuid::Uuid::new_v4().to_string())?;
    Ok(())
}

fn build_handlebars() -> Handlebars<'static> {
    let mut hb = Handlebars::new();
    hb.register_helper("ws_uuid", Box::new(ws_uuid_helper));
    hb
}
