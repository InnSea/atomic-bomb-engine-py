use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use url::Url;

#[derive(Debug, Eq, Clone, Serialize, Deserialize)]
pub struct AssertErrKey {
    pub name: String,
    pub msg: String,
    pub url: String,
    pub host: String,
    pub path: String,
}

impl PartialEq for AssertErrKey {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.url == other.url && self.msg == other.msg
    }
}

impl Hash for AssertErrKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        format!("{:?}{:?}{:?}", self.name, self.url, self.msg).hash(state);
    }
}

#[derive(Clone, Debug)]
pub struct AssertErrorStats {
    /// 内层用 parking_lot::Mutex: 临界区是一次 HashMap insert/inc, 纯 CPU,
    /// 不跨 await. 高并发下 (大量断言失败堆积) 用 async mutex 会让所有 task
    /// 走 tokio 争用队列, 显著放大调度抖动. 与 ws_error_stats 的优化对称.
    pub(crate) errors: Arc<Mutex<HashMap<AssertErrKey, u32>>>,
}

impl AssertErrorStats {
    pub(crate) fn new() -> Self {
        AssertErrorStats {
            errors: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// 增加一个错误和对应的出现次数. 不再是 async fn — 锁是同步的.
    pub(crate) fn increment(&self, name: String, msg: String, url_s: String) {
        let url = url_s.clone();
        let mut host = "-".to_string();
        let mut path = "-".to_string();
        if let Ok(u) = Url::parse(&url_s) {
            host = match u.host() {
                None => "-".to_string(),
                Some(h) => h.to_string(),
            };
            path = u.path().to_string();
        };

        let mut errors = self.errors.lock();
        *errors
            .entry(AssertErrKey {
                name,
                msg,
                url,
                host,
                path,
            })
            .or_insert(0) += 1;
    }
}
