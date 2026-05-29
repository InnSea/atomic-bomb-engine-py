//! send_pattern / heartbeat / reconnect / assert_option 构造器集合
use pyo3::types::{PyAny, PyDict, PyDictMethods};
use pyo3::{pyfunction, Py, PyResult, Python};

#[pyfunction]
#[pyo3(signature=(messages, rate_per_sec=None, interval_ms=None, iterate_data_pool=false))]
pub(crate) fn ws_send_pattern(
    py: Python,
    messages: Py<PyAny>,
    rate_per_sec: Option<f64>,
    interval_ms: Option<u64>,
    iterate_data_pool: bool,
) -> PyResult<Py<PyAny>> {
    let d = PyDict::new(py);
    d.set_item("messages", messages)?;
    if let Some(v) = rate_per_sec { d.set_item("rate_per_sec", v)?; }
    if let Some(v) = interval_ms { d.set_item("interval_ms", v)?; }
    d.set_item("iterate_data_pool", iterate_data_pool)?;
    Ok(d.into_any().unbind())
}

#[pyfunction]
#[pyo3(signature=(interval_secs, timeout_secs, payload=None))]
pub(crate) fn ws_heartbeat(
    py: Python,
    interval_secs: u64,
    timeout_secs: u64,
    payload: Option<Py<PyAny>>,
) -> PyResult<Py<PyAny>> {
    let d = PyDict::new(py);
    d.set_item("interval_secs", interval_secs)?;
    d.set_item("timeout_secs", timeout_secs)?;
    if let Some(v) = payload { d.set_item("payload", v)?; }
    Ok(d.into_any().unbind())
}

#[pyfunction]
pub(crate) fn ws_reconnect_policy(
    py: Python,
    max_attempts: u32,
    backoff_ms: u64,
) -> PyResult<Py<PyAny>> {
    let d = PyDict::new(py);
    d.set_item("max_attempts", max_attempts)?;
    d.set_item("backoff_ms", backoff_ms)?;
    Ok(d.into_any().unbind())
}

/// WS 断言, on_match 取值: "EveryMessage" | "FirstMatch" | "NoMatch", 默认 EveryMessage
#[pyfunction]
#[pyo3(signature=(jsonpath, reference_object, on_match=None))]
pub(crate) fn ws_assert_option(
    py: Python,
    jsonpath: String,
    reference_object: Py<PyAny>,
    on_match: Option<String>,
) -> PyResult<Py<PyAny>> {
    let d = PyDict::new(py);
    d.set_item("jsonpath", jsonpath)?;
    d.set_item("reference_object", reference_object)?;
    d.set_item("on_match", on_match.unwrap_or_else(|| "EveryMessage".to_string()))?;
    Ok(d.into_any().unbind())
}
