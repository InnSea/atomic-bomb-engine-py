use crate::utils;
use atomic_bomb_engine::models::result::BatchResult;
use futures::stream::BoxStream;
use futures::StreamExt;
use pyo3::types::{PyDict, PyList};
use pyo3::{pyclass, pymethods, PyObject, PyRefMut, PyResult, Python, ToPyObject};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use std::time::Duration;

#[pyclass]
pub(crate) struct BatchRunner {
    runtime: tokio::runtime::Runtime,
    stream: Arc<Mutex<Option<BoxStream<'static, Result<Option<BatchResult>, anyhow::Error>>>>>,
    is_done: Arc<Mutex<bool>>,
    should_stop: Arc<AtomicBool>,
    task_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
}

#[pymethods]
impl BatchRunner {
    #[new]
    fn new() -> Self {
        BatchRunner {
            runtime: tokio::runtime::Runtime::new().unwrap(),
            stream: Arc::new(Mutex::new(None)),
            is_done: Arc::new(Mutex::new(false)),
            should_stop: Arc::new(AtomicBool::new(false)),
            task_handle: Arc::new(Mutex::new(None)),
        }
    }

    fn stop(&self) {
        self.should_stop.store(true, Ordering::SeqCst);
        
        // 强制中止后台任务
        let task_handle = self.task_handle.clone();
        let stream_clone = self.stream.clone();
        let is_done_clone = self.is_done.clone();
        
        self.runtime.block_on(async move {
            // 中止任务句柄
            let mut handle_guard = task_handle.lock().await;
            if let Some(handle) = handle_guard.take() {
                handle.abort();
                // 等待任务真正结束
                let _ = tokio::time::timeout(Duration::from_millis(500), async {
                    // 给一点时间让任务清理
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }).await;
            }
            
            // 清理 stream
            let mut stream_lock = stream_clone.lock().await;
            *stream_lock = None;
            
            // 标记为完成
            let mut done_lock = is_done_clone.lock().await;
            *done_lock = true;
        });
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
    ))]
    fn run(
        &self,
        py: Python,
        test_duration_secs: u64,
        concurrent_requests: usize,
        api_endpoints: &PyList,
        step_option: Option<&PyDict>,
        setup_options: Option<&PyList>,
        verbose: bool,
        should_prevent: bool,
        assert_channel_buffer_size: usize,
        timeout_secs: u64,
        cookie_store_enable: bool,
        ema_alpha: f64,
    ) -> PyResult<PyObject> {
        let stream_clone = self.stream.clone();
        let task_handle_clone = self.task_handle.clone();
        let should_stop_clone = self.should_stop.clone();
        
        let endpoints = utils::parse_api_endpoints::new(py, api_endpoints)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        let step_opt = utils::parse_step_options::new(step_option)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        let setup_opts = utils::parse_setup_options::new(py, setup_options)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        let fut = async move {
            // 重置停止标志
            should_stop_clone.store(false, Ordering::SeqCst);
            
            // 启动后台任务
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
                )
                .await;
                *stream_clone.lock().await = Some(stream);
            });
            
            // 保存任务句柄
            *task_handle_clone.lock().await = Some(handle);
            
            Ok::<(), pyo3::PyErr>(())
        };

        Python::with_gil(|py| {
            pyo3_asyncio::tokio::future_into_py(py, fut).map(|py_any| py_any.to_object(py))
        })
    }

    fn __iter__(slf: PyRefMut<Self>) -> PyResult<PyRefMut<Self>> {
        Ok(slf)
    }

    fn __next__(slf: PyRefMut<'_, Self>, py: Python) -> PyResult<Option<PyObject>> {
        let is_done_clone = slf.is_done.clone();

        // 检查停止标志
        if slf.should_stop.load(Ordering::SeqCst) {
            slf.runtime.block_on(async {
                let mut done_lock = is_done_clone.lock().await;
                *done_lock = true;
                // 清理 stream，释放资源
                let mut stream_lock = slf.stream.lock().await;
                *stream_lock = None;
            });
            return Ok(None);
        }

        let is_done = slf.runtime.block_on(async {
            let done = is_done_clone.lock().await;
            *done
        });

        if is_done {
            return Ok(None);
        }

        let mut stream_guard = slf.runtime.block_on(async {
            slf.stream.lock().await
        });

        match stream_guard.as_mut() {
            Some(stream) => {
                let should_stop = slf.should_stop.clone();
                let next_stream = slf.runtime.block_on(async {
                    // 再次检查停止标志
                    if should_stop.load(Ordering::SeqCst) {
                        return None;
                    }
                    stream.next().await
                });

                match next_stream {
                    Some(Ok(result)) => {
                        if result.is_none() {
                            let done = slf.is_done.clone();
                            slf.runtime.block_on(async {
                                let mut done_lock = done.lock().await;
                                *done_lock = true;
                            });
                        }

                        let dict = PyDict::new(py);
                        if let Some(test_result) = result {
                            dict.set_item("total_duration", test_result.total_duration)?;
                            dict.set_item("success_rate", test_result.success_rate)?;
                            dict.set_item("error_rate", test_result.error_rate)?;
                            dict.set_item(
                                "median_response_time",
                                test_result.median_response_time,
                            )?;
                            dict.set_item("response_time_95", test_result.response_time_95)?;
                            dict.set_item("response_time_99", test_result.response_time_99)?;
                            dict.set_item("total_requests", test_result.total_requests)?;
                            dict.set_item("rps", test_result.rps)?;
                            dict.set_item("max_response_time", test_result.max_response_time)?;
                            dict.set_item("min_response_time", test_result.min_response_time)?;
                            dict.set_item("err_count", test_result.err_count)?;
                            dict.set_item("total_data_kb", test_result.total_data_kb)?;
                            dict.set_item(
                                "throughput_per_second_kb",
                                test_result.throughput_per_second_kb,
                            )?;
                            let http_error_list =
                                utils::create_http_err_dict::create_http_error_dict(
                                    py,
                                    &test_result.http_errors,
                                )?;
                            dict.set_item("http_errors", http_error_list)?;
                            let assert_error_list =
                                utils::create_assert_err_dict::create_assert_error_dict(
                                    py,
                                    &test_result.assert_errors,
                                )?;
                            dict.set_item("assert_errors", assert_error_list)?;
                            dict.set_item("timestamp", test_result.timestamp)?;
                            let api_results =
                                utils::create_api_results_dict::create_api_results_dict(
                                    py,
                                    test_result.api_results,
                                )?;
                            dict.set_item("api_results", api_results)?;
                            dict.set_item(
                                "total_concurrent_number",
                                test_result.total_concurrent_number,
                            )?;
                            dict.set_item("errors_per_second", test_result.errors_per_second)?;
                        };
                        Ok(Some(dict.to_object(py)))
                    }
                    Some(Err(e)) => Err(pyo3::exceptions::PyRuntimeError::new_err(e.to_string())),
                    None => {
                        let done = slf.is_done.clone();
                        slf.runtime.block_on(async {
                            let mut done_lock = done.lock().await;
                            *done_lock = true;
                        });
                        Ok(None)
                    }
                }
            }
            None => {
                eprintln!("stream未初始化，请等待");
                let dict = PyDict::new(py);
                dict.set_item("should_wait", true)?;
                Ok(Some(dict.to_object(py)))
            }
        }
    }
}
