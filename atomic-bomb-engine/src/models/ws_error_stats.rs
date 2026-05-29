use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// WebSocket 错误分类
#[derive(Debug, Eq, Clone, Serialize, Deserialize)]
pub enum WsErrKind {
    /// TLS / TCP / HTTP 升级阶段错误
    Handshake,
    /// 连接已建立后收发出错 (协议错误 / IO 错误)
    Stream,
    /// 心跳超时
    HeartbeatTimeout,
    /// 服务端关闭, 携带 close code
    ServerClose,
    /// 配对超时 (RequestResponse 模式)
    MatchTimeout,
    /// 序列化 / 渲染 / 自定义业务错误
    Internal,
}

impl PartialEq for WsErrKind {
    fn eq(&self, other: &Self) -> bool {
        std::mem::discriminant(self) == std::mem::discriminant(other)
    }
}

impl Hash for WsErrKind {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
    }
}

#[derive(Debug, Eq, Clone, Serialize, Deserialize)]
pub struct WsErrKey {
    pub name: String,
    pub url: String,
    pub host: String,
    pub kind: WsErrKind,
    pub close_code: u16,
    pub msg: String,
}

impl PartialEq for WsErrKey {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.url == other.url
            && self.kind == other.kind
            && self.close_code == other.close_code
            && self.msg == other.msg
    }
}

impl Hash for WsErrKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        format!(
            "{}|{}|{:?}|{}|{}",
            self.name, self.url, self.kind, self.close_code, self.msg
        )
        .hash(state);
    }
}

pub struct WsErrorStats {
    /// 用 parking_lot::Mutex 而非 tokio::Mutex: 临界区只是 HashMap 上的一次
    /// hash + insert/inc, 纯 CPU, 持锁不跨 await. 高并发下 (3000+ 连接同时
    /// 命中同类错误) 用 async mutex 会让所有 task 走 tokio 的争用队列, 实测
    /// 能放大成秒级抖动. parking_lot 的自旋+park 路径在短临界区上明显更快.
    pub(crate) errors: Arc<Mutex<HashMap<WsErrKey, u32>>>,
}

impl WsErrorStats {
    pub fn new() -> Self {
        Self {
            errors: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// 注: 不再是 async fn — 临界区是同步的, 没有任何 await
    ///
    /// host 由调用方在连接建立时计算一次后传入, 避免每次错误都做一次 Url::parse.
    /// 高并发错误路径上这能省掉数千次 URL 解析.
    pub fn increment(
        &self,
        name: String,
        url: String,
        host: String,
        kind: WsErrKind,
        close_code: u16,
        msg: String,
    ) {
        let mut errors = self.errors.lock();
        *errors
            .entry(WsErrKey {
                name,
                url,
                host,
                kind,
                close_code,
                msg,
            })
            .or_insert(0) += 1;
    }
}

/// 提取 url 的 host 部分, 用于错误归类的缓存键. 解析失败时返回 "-".
pub fn host_of(url: &str) -> String {
    match url::Url::parse(url) {
        Ok(u) => u.host_str().unwrap_or("-").to_string(),
        Err(_) => "-".to_string(),
    }
}
