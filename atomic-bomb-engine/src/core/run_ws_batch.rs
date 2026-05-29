use crate::core::ws_batch;
use crate::models::data_pool::DataPool;
use crate::models::setup::SetupApiEndpoint;
use crate::models::step_option::StepOption;
use crate::models::ws_endpoint::WsEndpoint;
use crate::models::ws_result::WsBatchResult;
use futures::stream::{BoxStream, StreamExt};
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tokio::sync::mpsc;

/// 与 HTTP 的 run_batch 完全独立的 WS 入口
///
/// 输出 BoxStream<WsBatchResult>, Python 侧的 WsBatchRunner 直接消费
pub async fn run_ws_batch(
    test_duration_secs: u64,
    concurrent_connections: usize,
    timeout_secs: u64,
    cookie_store_enable: bool,
    verbose: bool,
    should_prevent: bool,
    ws_endpoints: Vec<WsEndpoint>,
    step_option: Option<StepOption>,
    setup_options: Option<Vec<SetupApiEndpoint>>,
    assert_channel_buffer_size: usize,
    should_stop: Option<Arc<AtomicBool>>,
    data_pool: Option<Arc<DataPool>>,
    global_variables: Option<BTreeMap<String, Value>>,
    teardown_options: Option<Vec<SetupApiEndpoint>>,
) -> BoxStream<'static, Result<Option<WsBatchResult>, anyhow::Error>> {
    let (sender, receiver) = mpsc::channel(1024);

    tokio::spawn(async move {
        let res = ws_batch::ws_batch(
            sender.clone(),
            test_duration_secs,
            concurrent_connections,
            timeout_secs,
            cookie_store_enable,
            verbose,
            should_prevent,
            ws_endpoints,
            step_option,
            setup_options,
            assert_channel_buffer_size,
            should_stop,
            data_pool,
            global_variables,
            teardown_options,
        )
        .await;
        match res {
            Ok(r) => {
                let _ = sender.send(Some(r)).await;
                let _ = sender.send(None).await;
            }
            Err(e) => {
                let err_result = WsBatchResult::empty_with_engine_errors(vec![e.to_string()]);
                let _ = sender.send(Some(err_result)).await;
                let _ = sender.send(None).await;
            }
        }
    });

    let stream = futures::stream::unfold(receiver, |mut rx| async move {
        match rx.recv().await {
            Some(Some(r)) => Some((Ok(Some(r)), rx)),
            Some(None) => Some((Ok(None), rx)),
            None => None,
        }
    });
    stream.boxed()
}
