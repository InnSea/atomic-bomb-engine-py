//! WS 消息模板构造器
//!
//! 三个函数: ws_text / ws_binary / ws_json, 对应三种消息载荷
use pyo3::types::{PyAny, PyDict, PyDictMethods};
use pyo3::{pyfunction, Py, PyResult, Python};

#[pyfunction]
pub(crate) fn ws_text(py: Python, value: String) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    dict.set_item("type", "Text")?;
    dict.set_item("value", value)?;
    Ok(dict.into_any().unbind())
}

#[pyfunction]
pub(crate) fn ws_binary(py: Python, value: Vec<u8>) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    dict.set_item("type", "Binary")?;
    dict.set_item("value", value)?;
    Ok(dict.into_any().unbind())
}

#[pyfunction]
pub(crate) fn ws_json(py: Python, value: Py<PyAny>) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    dict.set_item("type", "Json")?;
    dict.set_item("value", value)?;
    Ok(dict.into_any().unbind())
}
