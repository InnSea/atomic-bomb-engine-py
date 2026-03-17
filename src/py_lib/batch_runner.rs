use crate::utils;
use atomic_bomb_engine::models::result::BatchResult;
use futures::stream::BoxStream;
use futures::StreamExt;
use pyo3::types::{PyAny, PyDict, PyDictMethods, PyList};
use pyo3::{pyclass, pymethods, Py, PyRefMut, PyResult, Python};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

/// 将 BatchResult 转换为 Python dict
fn batch_result_to_dict(py: Python, test_result: BatchResult) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    dict.set_item("total_duration", test_result.total_duration)?;
    dict.set_item("success_rate", test_result.success_rate)?;
    dict.set_item("error_rate", test_result.error_rate)?;
    dict.set_item("median_response_time", test_result.median_response_time)?;
    dict.set_item("response_time_95", test_result.response_time_95)?;
    dict.set_item("response_time_99", test_result.response_time_99)?;
    dict.set_item("total_requests", test_result.total_requests)?;
    dict.set_item("rps", test_result.rps)?;
    dict.set_item("max_response_time", test_result.max_response_time)?;
    dict.set_item("min_response_time", test_result.min_response_time)?;
    dict.set_item("err_count", test_result.err_count)?;
    dict.set_item("total_data_kb", test_result.total_data_kb)?;
    dict.set_item("throughput_per_second_kb", test_result.throughput_per_second_kb)?;
    let http_error_list =
        utils::create_http_err_dict::create_http_error_dict(py, &test_result.http_errors)?;
    dict.set_item("http_errors", http_error_list)?;
    let assert_error_list =
        utils::create_assert_err_dict::create_assert_error_dict(py, &test_result.assert_errors)?;
    dict.set_item("assert_errors", assert_error_list)?;
    dict.set_item("timestamp", test_result.timestamp)?;
    let api_results =
        utils::create_api_results_dict::create_api_results_dict(py, test_result.api_results)?;
    dict.set_item("api_results", api_results)?;
    dict.set_item("total_concurrent_number", test_result.total_concurrent_number)?;
    dict.set_item("errors_per_second", test_result.errors_per_second)?;
    dict.set_item("avg_response_time", test_result.avg_response_time)?;
    let error_list = PyList::new(py, &test_result.engine_errors)?;
    dict.set_item("engine_errors", error_list)?;
    if let Some(ref dp_stats) = test_result.data_pool_stats {
        let dp_dict = PyDict::new(py);
        dp_dict.set_item("total_rows", dp_stats.total_rows)?;
        dp_dict.set_item("mode", &dp_stats.mode)?;
        dp_dict.set_item("cycles", dp_stats.cycles)?;
        dict.set_item("data_pool_stats", dp_dict)?;
    }
    Ok(dict.into_any().unbind())
}

/// async future 的返回值
enum StreamItem {
    Data(BatchResult),
    WaitInit,
    Done,
}

#[pyclass]
pub(crate) struct BatchRunner {
    /// 独立 runtime，专门用于 poll stream（同步迭代 + 异步迭代的 stream 消费）
    /// 与压测引擎所在的全局 runtime 隔离，避免高并发时互相抢线程
    runtime: Arc<tokio::runtime::Runtime>,
    stream: Arc<Mutex<Option<BoxStream<'static, Result<Option<BatchResult>, anyhow::Error>>>>>,
    is_done: Arc<AtomicBool>,
    should_stop: Arc<AtomicBool>,
    task_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    engine_should_stop: Arc<AtomicBool>,
}

#[pymethods]
impl BatchRunner {
    #[new]
    fn new() -> Self {
        BatchRunner {
            runtime: Arc::new(tokio::runtime::Runtime::new().unwrap()),
            stream: Arc::new(Mutex::new(None)),
            is_done: Arc::new(AtomicBool::new(false)),
            should_stop: Arc::new(AtomicBool::new(false)),
            task_handle: Arc::new(Mutex::new(None)),
            engine_should_stop: Arc::new(AtomicBool::new(false)),
        }
    }

    /// 异步停止压测
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
    concurrent_requests,
    api_endpoints,
    step_option=None,
    setup_options=None,
    verbose=false,
    should_prevent=false,
    assert_channel_buffer_size=1024,
    timeout_secs=0,
    cookie_store_enable=true,
    ema_alpha=0f64,
    data_pool=None,
    global_variables=None,
    teardown_options=None,
    ))]
    fn run(
        &self,
        py: Python,
        test_duration_secs: u64,
        concurrent_requests: usize,
        api_endpoints: Py<PyList>,
        step_option: Option<Py<PyDict>>,
        setup_options: Option<Py<PyList>>,
        verbose: bool,
        should_prevent: bool,
        assert_channel_buffer_size: usize,
        timeout_secs: u64,
        cookie_store_enable: bool,
        ema_alpha: f64,
        data_pool: Option<Py<PyDict>>,
        global_variables: Option<Py<PyDict>>,
        teardown_options: Option<Py<PyList>>,
    ) -> PyResult<Py<PyAny>> {
        let stream_clone = self.stream.clone();
        let task_handle_clone = self.task_handle.clone();
        let should_stop_clone = self.should_stop.clone();
        let is_done_clone = self.is_done.clone();
        let engine_should_stop_clone = self.engine_should_stop.clone();

        let endpoints = utils::parse_api_endpoints::new(py, api_endpoints)
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
            // 重置标志
            should_stop_clone.store(false, Ordering::SeqCst);
            engine_should_stop_clone.store(false, Ordering::SeqCst);
            is_done_clone.store(false, Ordering::SeqCst);

            let handle = tokio::spawn(async move {
                let stream = atomic_bomb_engine::core::run_batch::run_batch(
                    test_duration_secs,
                    concurrent_requests,
                    timeout_secs,
                    cookie_store_enable,
                    verbose,
                    should_prevent,
                    endpoints,
                    step_opt,
                    setup_opts,
                    assert_channel_buffer_size,
                    ema_alpha,
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

    // ========== 异步迭代器协议 (async for) ==========

    fn __aiter__(slf: PyRefMut<Self>) -> PyRefMut<Self> {
        slf
    }

    fn __anext__(&self, py: Python) -> PyResult<Option<Py<PyAny>>> {
        let is_done = self.is_done.clone();
        let stream_clone = self.stream.clone();
        let should_stop = self.should_stop.clone();
        let rt = self.runtime.clone();

        // 在独立 runtime 上 spawn 一个任务来 poll stream，
        // 通过 oneshot 把结果发回给 future_into_py 的 future
        let (tx, rx) = tokio::sync::oneshot::channel::<Result<StreamItem, String>>();

        rt.spawn(async move {
            let item = if should_stop.load(Ordering::SeqCst) || is_done.load(Ordering::SeqCst) {
                is_done.store(true, Ordering::SeqCst);
                Ok(StreamItem::Done)
            } else {
                let next_item = {
                    let mut guard = stream_clone.lock().await;
                    match guard.as_mut() {
                        Some(stream) => stream.next().await,
                        None => {
                            let _ = tx.send(Ok(StreamItem::WaitInit));
                            return;
                        }
                    }
                };
                match next_item {
                    Some(Ok(Some(result))) => Ok(StreamItem::Data(result)),
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

        // future_into_py 的 future 只等 oneshot 结果 + 做 Python 转换
        let coroutine = pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let item = rx
                .await
                .map_err(|_| {
                    pyo3::exceptions::PyRuntimeError::new_err("stream task dropped")
                })?
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e))?;

            match item {
                StreamItem::Data(result) => {
                    let dict = Python::attach(|py| batch_result_to_dict(py, result))?;
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

    // ========== 同步迭代器协议 (for) - 向后兼容 ==========

    fn __iter__(slf: PyRefMut<Self>) -> PyResult<PyRefMut<Self>> {
        Ok(slf)
    }

    fn __next__(slf: PyRefMut<'_, Self>, py: Python) -> PyResult<Option<Py<PyAny>>> {
        if slf.should_stop.load(Ordering::SeqCst) {
            slf.is_done.store(true, Ordering::SeqCst);
            slf.runtime.block_on(async {
                let mut stream_lock = slf.stream.lock().await;
                *stream_lock = None;
            });
            return Ok(None);
        }

        if slf.is_done.load(Ordering::SeqCst) {
            return Ok(None);
        }

        let mut stream_guard = slf.runtime.block_on(async { slf.stream.lock().await });

        match stream_guard.as_mut() {
            Some(stream) => {
                let should_stop = slf.should_stop.clone();
                let next_stream = slf.runtime.block_on(async {
                    if should_stop.load(Ordering::SeqCst) {
                        return None;
                    }
                    stream.next().await
                });

                match next_stream {
                    Some(Ok(result)) => {
                        if result.is_none() {
                            slf.is_done.store(true, Ordering::SeqCst);
                        }
                        let dict = PyDict::new(py);
                        if let Some(test_result) = result {
                            return batch_result_to_dict(py, test_result).map(Some);
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
                let dict = PyDict::new(py);
                dict.set_item("should_wait", true)?;
                Ok(Some(dict.into_any().unbind()))
            }
        }
    }
}
