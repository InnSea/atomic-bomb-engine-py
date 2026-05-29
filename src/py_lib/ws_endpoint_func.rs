//! WS endpoint 构造函数, 与 endpoint() 平级, 但用于 WebSocket 压测
use pyo3::types::{PyAny, PyDict, PyDictMethods};
use pyo3::{pyfunction, Py, PyResult, Python};

#[pyfunction]
#[pyo3(signature=(
    name,
    url,
    weight,
    mode,
    subprotocols=None,
    headers=None,
    on_connect=None,
    send_pattern=None,
    heartbeat=None,
    connection_ttl_secs=None,
    reconnect=None,
    assert_options=None,
    think_time_option=None,
    setup_options=None,
    teardown_options=None,
))]
pub(crate) fn ws_endpoint(
    py: Python,
    name: String,
    url: String,
    weight: u32,
    mode: Py<PyAny>,
    subprotocols: Option<Py<PyAny>>,
    headers: Option<Py<PyAny>>,
    on_connect: Option<Py<PyAny>>,
    send_pattern: Option<Py<PyAny>>,
    heartbeat: Option<Py<PyAny>>,
    connection_ttl_secs: Option<u64>,
    reconnect: Option<Py<PyAny>>,
    assert_options: Option<Py<PyAny>>,
    think_time_option: Option<Py<PyAny>>,
    setup_options: Option<Py<PyAny>>,
    teardown_options: Option<Py<PyAny>>,
) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    dict.set_item("name", name)?;
    dict.set_item("url", url)?;
    dict.set_item("weight", weight)?;
    dict.set_item("mode", mode)?;
    if let Some(v) = subprotocols { dict.set_item("subprotocols", v)?; }
    if let Some(v) = headers { dict.set_item("headers", v)?; }
    if let Some(v) = on_connect { dict.set_item("on_connect", v)?; }
    if let Some(v) = send_pattern { dict.set_item("send_pattern", v)?; }
    if let Some(v) = heartbeat { dict.set_item("heartbeat", v)?; }
    if let Some(v) = connection_ttl_secs { dict.set_item("connection_ttl_secs", v)?; }
    if let Some(v) = reconnect { dict.set_item("reconnect", v)?; }
    if let Some(v) = assert_options { dict.set_item("assert_options", v)?; }
    if let Some(v) = think_time_option { dict.set_item("think_time_option", v)?; }
    if let Some(v) = setup_options { dict.set_item("setup_options", v)?; }
    if let Some(v) = teardown_options { dict.set_item("teardown_options", v)?; }
    Ok(dict.into_any().unbind())
}
