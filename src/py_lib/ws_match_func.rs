//! 配对策略 / 模式构造
use pyo3::types::{PyAny, PyDict, PyDictMethods};
use pyo3::{pyfunction, Py, PyResult, Python};

/// 顺序匹配 (FIFO)
#[pyfunction]
pub(crate) fn ws_match_sequential(py: Python) -> PyResult<Py<PyAny>> {
    let d = PyDict::new(py);
    d.set_item("type", "Sequential")?;
    Ok(d.into_any().unbind())
}

/// JSONPath 匹配
#[pyfunction]
pub(crate) fn ws_match_jsonpath(
    py: Python,
    send_path: String,
    recv_path: String,
) -> PyResult<Py<PyAny>> {
    let d = PyDict::new(py);
    d.set_item("type", "JsonPath")?;
    d.set_item("send_path", send_path)?;
    d.set_item("recv_path", recv_path)?;
    Ok(d.into_any().unbind())
}

/// OneWay 模式
#[pyfunction]
pub(crate) fn ws_mode_oneway(py: Python) -> PyResult<Py<PyAny>> {
    let d = PyDict::new(py);
    d.set_item("type", "OneWay")?;
    Ok(d.into_any().unbind())
}

/// RequestResponse 模式
#[pyfunction]
#[pyo3(signature=(match_by, timeout_ms=30_000))]
pub(crate) fn ws_mode_request_response(
    py: Python,
    match_by: Py<PyAny>,
    timeout_ms: u64,
) -> PyResult<Py<PyAny>> {
    let d = PyDict::new(py);
    d.set_item("type", "RequestResponse")?;
    d.set_item("match_by", match_by)?;
    d.set_item("timeout_ms", timeout_ms)?;
    Ok(d.into_any().unbind())
}
