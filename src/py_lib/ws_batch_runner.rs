//! WsBatchRunner: WebSocket 压测的 Python 入口类
//!
//! 与 BatchRunner 同构, 但完全独立: 独立 runtime / stream / stop 标志, 互不影响
use crate::utils;
use atomic_bomb_engine::models::ws_result::WsBatchResult;
use futures::stream::BoxStream;
use futures::StreamExt;
use pyo3::types::{PyAny, PyDict, PyDictMethods, PyList};
use pyo3::{pyclass, pymethods, Py, PyRefMut, PyResult, Python};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

enum StreamItem {
    Data(WsBatchResult),
    WaitInit,
    Done,
}

#[pyclass]
pub(crate) struct WsBatchRunner {
    engine_runtime: Arc<tokio::runtime::Runtime>,
    runtime: Arc<tokio::runtime::Runtime>,
    stream: Arc<Mutex<Option<BoxStream<'static, Result<Option<WsBatchResult>, anyhow::Error>>>>>,
    is_done: Arc<AtomicBool>,
    should_stop: Arc<AtomicBool>,
    task_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    engine_should_stop: Arc<AtomicBool>,
}

fn build_engine_runtime() -> tokio::runtime::Runtime {
    let cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    let workers = (cpus * 2).max(4);
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .thread_name("abe-ws-engine")
        .enable_all()
        .build()
        .expect("构建 WS 引擎 runtime 失败")
}

#[pymethods]
impl WsBatchRunner {
    #[new]
    fn new() -> Self {
        WsBatchRunner {
            engine_runtime: Arc::new(build_engine_runtime()),
            runtime: Arc::new(tokio::runtime::Runtime::new().unwrap()),
            stream: Arc::new(Mutex::new(None)),
            is_done: Arc::new(AtomicBool::new(false)),
            should_stop: Arc::new(AtomicBool::new(false)),
            task_handle: Arc::new(Mutex::new(None)),
            engine_should_stop: Arc::new(AtomicBool::new(false)),
        }
    }

    fn stop<'py>(&self, py: Python<'py>) -> PyResult<Py<PyAny>> {
        self.should_stop.store(true, Ordering::SeqCst);
        self.engine_should_stop.store(true, Ordering::SeqCst);

        let task_handle = self.task_handle.clone();
        let stream_clone = self.stream.clone();
        let is_done_clone = self.is_done.clone();

        let fut = async move {
            let mut handle_guard = task_handle.lock().await;
            if let Some(handle) = handle_guard.take() {
                handle.abort();
                let _ = tokio::time::timeout(Duration::from_millis(500), async {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                })
                .await;
            }
            let mut stream_lock = stream_clone.lock().await;
            *stream_lock = None;
            is_done_clone.store(true, Ordering::SeqCst);
            Ok(())
        };

        pyo3_async_runtimes::tokio::future_into_py(py, fut).map(|py_any| py_any.unbind())
    }

    #[pyo3(signature = (
        test_duration_secs,
        concurrent_connections,
        ws_endpoints,
        step_option=None,
        setup_options=None,
        verbose=false,
        should_prevent=false,
        assert_channel_buffer_size=1024,
        timeout_secs=0,
        cookie_store_enable=true,
        data_pool=None,
        global_variables=None,
        teardown_options=None,
    ))]
    fn run(
        &self,
        py: Python,
        test_duration_secs: u64,
        concurrent_connections: usize,
        ws_endpoints: Py<PyList>,
        step_option: Option<Py<PyDict>>,
        setup_options: Option<Py<PyList>>,
        verbose: bool,
        should_prevent: bool,
        assert_channel_buffer_size: usize,
        timeout_secs: u64,
        cookie_store_enable: bool,
        data_pool: Option<Py<PyDict>>,
        global_variables: Option<Py<PyDict>>,
        teardown_options: Option<Py<PyList>>,
    ) -> PyResult<Py<PyAny>> {
        let stream_clone = self.stream.clone();
        let task_handle_clone = self.task_handle.clone();
        let should_stop_clone = self.should_stop.clone();
        let is_done_clone = self.is_done.clone();
        let engine_should_stop_clone = self.engine_should_stop.clone();
        let engine_rt = self.engine_runtime.clone();

        let endpoints = utils::parse_ws_endpoints::new(py, ws_endpoints)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        let step_opt = utils::parse_step_options::new(py, step_option)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        let setup_opts = utils::parse_setup_options::new(py, setup_options)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        let data_pool_opt = utils::parse_data_pool::new(py, data_pool)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        let global_vars = utils::parse_global_variables::new(py, global_variables)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        let teardown_opts = utils::parse_setup_options::new(py, teardown_options)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        let fut = async move {
            should_stop_clone.store(false, Ordering::SeqCst);
            engine_should_stop_clone.store(false, Ordering::SeqCst);
            is_done_clone.store(false, Ordering::SeqCst);

            // 关键: 把引擎 task spawn 到独立的 engine_runtime, 而不是当前 (pyo3)
            // runtime, 避免 3000+ 连接占满 Python 异步消费者所在的 worker pool.
            let handle = engine_rt.spawn(async move {
                let stream = atomic_bomb_engine::core::run_ws_batch::run_ws_batch(
                    test_duration_secs,
                    concurrent_connections,
                    timeout_secs,
                    cookie_store_enable,
                    verbose,
                    should_prevent,
                    endpoints,
                    step_opt,
                    setup_opts,
                    assert_channel_buffer_size,
                    Some(engine_should_stop_clone),
                    data_pool_opt,
                    global_vars,
                    teardown_opts,
                )
                .await;
                *stream_clone.lock().await = Some(stream);
            });

            *task_handle_clone.lock().await = Some(handle);
            Ok::<(), pyo3::PyErr>(())
        };

        Python::attach(|py| {
            pyo3_async_runtimes::tokio::future_into_py(py, fut).map(|py_any| py_any.unbind())
        })
    }

    fn __aiter__(slf: PyRefMut<Self>) -> PyRefMut<Self> {
        slf
    }

    fn __anext__(&self, py: Python) -> PyResult<Option<Py<PyAny>>> {
        let is_done = self.is_done.clone();
        let stream_clone = self.stream.clone();
        let should_stop = self.should_stop.clone();
        let rt = self.runtime.clone();

        let (tx, rx) = tokio::sync::oneshot::channel::<Result<StreamItem, String>>();

        rt.spawn(async move {
            let item = if should_stop.load(Ordering::SeqCst) || is_done.load(Ordering::SeqCst) {
                is_done.store(true, Ordering::SeqCst);
                Ok(StreamItem::Done)
            } else {
                let next = {
                    let mut g = stream_clone.lock().await;
                    match g.as_mut() {
                        Some(s) => s.next().await,
                        None => {
                            let _ = tx.send(Ok(StreamItem::WaitInit));
                            return;
                        }
                    }
                };
                match next {
                    Some(Ok(Some(r))) => Ok(StreamItem::Data(r)),
                    Some(Ok(None)) => {
                        is_done.store(true, Ordering::SeqCst);
                        Ok(StreamItem::Done)
                    }
                    Some(Err(e)) => Err(e.to_string()),
                    None => {
                        is_done.store(true, Ordering::SeqCst);
                        Ok(StreamItem::Done)
                    }
                }
            };
            let _ = tx.send(item);
        });

        let coroutine = pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let item = rx
                .await
                .map_err(|_| pyo3::exceptions::PyRuntimeError::new_err("ws stream task dropped"))?
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e))?;

            match item {
                StreamItem::Data(r) => {
                    let dict = Python::attach(|py| {
                        utils::create_ws_results_dict::ws_batch_result_to_dict(py, r)
                    })?;
                    Ok(dict)
                }
                StreamItem::WaitInit => {
                    let dict = Python::attach(|py| -> PyResult<Py<PyAny>> {
                        let d = PyDict::new(py);
                        d.set_item("should_wait", true)?;
                        Ok(d.into_any().unbind())
                    })?;
                    Ok(dict)
                }
                StreamItem::Done => Err(pyo3::exceptions::PyStopAsyncIteration::new_err(())),
            }
        })
        .map(|py_any| py_any.unbind())?;

        Ok(Some(coroutine))
    }

    fn __iter__(slf: PyRefMut<Self>) -> PyResult<PyRefMut<Self>> {
        Ok(slf)
    }

    fn __next__(slf: PyRefMut<'_, Self>, py: Python) -> PyResult<Option<Py<PyAny>>> {
        if slf.should_stop.load(Ordering::SeqCst) {
            slf.is_done.store(true, Ordering::SeqCst);
            slf.runtime.block_on(async {
                let mut s = slf.stream.lock().await;
                *s = None;
            });
            return Ok(None);
        }
        if slf.is_done.load(Ordering::SeqCst) {
            return Ok(None);
        }

        let mut g = slf.runtime.block_on(async { slf.stream.lock().await });
        match g.as_mut() {
            Some(stream) => {
                let should_stop = slf.should_stop.clone();
                let nxt = slf.runtime.block_on(async {
                    if should_stop.load(Ordering::SeqCst) {
                        return None;
                    }
                    stream.next().await
                });
                match nxt {
                    Some(Ok(r)) => {
                        if r.is_none() {
                            slf.is_done.store(true, Ordering::SeqCst);
                        }
                        let dict = PyDict::new(py);
                        if let Some(rr) = r {
                            return utils::create_ws_results_dict::ws_batch_result_to_dict(py, rr)
                                .map(Some);
                        }
                        Ok(Some(dict.into_any().unbind()))
                    }
                    Some(Err(e)) => Err(pyo3::exceptions::PyRuntimeError::new_err(e.to_string())),
                    None => {
                        slf.is_done.store(true, Ordering::SeqCst);
                        Ok(None)
                    }
                }
            }
            None => {
                let d = PyDict::new(py);
                d.set_item("should_wait", true)?;
                Ok(Some(d.into_any().unbind()))
            }
        }
    }
}
