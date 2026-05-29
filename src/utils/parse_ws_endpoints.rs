//! 将 Python list[dict] 解析为 Vec<WsEndpoint>
use crate::utils;
use crate::utils::depythonize::depythonize;
use atomic_bomb_engine::models::api_endpoint::ThinkTime;
use atomic_bomb_engine::models::ws_endpoint::{
    AssertTrigger, Heartbeat, MessageMatcher, ReconnectPolicy, SendPattern, WsAssertOption,
    WsEndpoint, WsMessageTemplate, WsMode,
};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyDict, PyList, PyListMethods};
use serde_json::Value;
use std::collections::HashMap;

fn parse_message_template(v: &Bound<PyAny>) -> PyResult<WsMessageTemplate> {
    let dict = v.cast::<PyDict>()?;
    let ty: String = dict
        .get_item("type")?
        .ok_or_else(|| PyErr::new::<PyRuntimeError, _>("ws message: type 不能为空".to_string()))?
        .extract()?;
    let value = dict
        .get_item("value")?
        .ok_or_else(|| PyErr::new::<PyRuntimeError, _>("ws message: value 不能为空".to_string()))?;
    Ok(match ty.as_str() {
        "Text" => WsMessageTemplate::Text(value.extract::<String>()?),
        "Binary" => WsMessageTemplate::Binary(value.extract::<Vec<u8>>()?),
        "Json" => {
            let json_val: Value = depythonize(&value)?;
            WsMessageTemplate::Json(json_val)
        }
        other => {
            return Err(PyErr::new::<PyRuntimeError, _>(format!(
                "未知的 ws message 类型: {}",
                other
            )))
        }
    })
}

fn parse_message_template_list(v: &Bound<PyAny>) -> PyResult<Vec<WsMessageTemplate>> {
    let list = v.cast::<PyList>()?;
    let mut out = Vec::with_capacity(list.len());
    for item in list.iter() {
        out.push(parse_message_template(&item)?);
    }
    Ok(out)
}

fn parse_mode(v: &Bound<PyAny>) -> PyResult<WsMode> {
    let dict = v.cast::<PyDict>()?;
    let ty: String = dict
        .get_item("type")?
        .ok_or_else(|| PyErr::new::<PyRuntimeError, _>("mode.type 不能为空".to_string()))?
        .extract()?;
    Ok(match ty.as_str() {
        "OneWay" => WsMode::OneWay,
        "RequestResponse" => {
            let mb_any = dict.get_item("match_by")?.ok_or_else(|| {
                PyErr::new::<PyRuntimeError, _>("RequestResponse 模式需要 match_by".to_string())
            })?;
            let match_by = parse_matcher(&mb_any)?;
            let timeout_ms: u64 = match dict.get_item("timeout_ms")? {
                Some(t) => t.extract().unwrap_or(30_000),
                None => 30_000,
            };
            WsMode::RequestResponse {
                match_by,
                timeout_ms,
            }
        }
        other => {
            return Err(PyErr::new::<PyRuntimeError, _>(format!(
                "未知的 ws mode: {}",
                other
            )))
        }
    })
}

fn parse_matcher(v: &Bound<PyAny>) -> PyResult<MessageMatcher> {
    let dict = v.cast::<PyDict>()?;
    let ty: String = dict
        .get_item("type")?
        .ok_or_else(|| PyErr::new::<PyRuntimeError, _>("matcher.type 不能为空".to_string()))?
        .extract()?;
    Ok(match ty.as_str() {
        "Sequential" => MessageMatcher::Sequential,
        "JsonPath" => {
            let send_path: String = dict
                .get_item("send_path")?
                .ok_or_else(|| {
                    PyErr::new::<PyRuntimeError, _>("JsonPath matcher 需要 send_path".to_string())
                })?
                .extract()?;
            let recv_path: String = dict
                .get_item("recv_path")?
                .ok_or_else(|| {
                    PyErr::new::<PyRuntimeError, _>("JsonPath matcher 需要 recv_path".to_string())
                })?
                .extract()?;
            MessageMatcher::JsonPath {
                send_path,
                recv_path,
            }
        }
        other => {
            return Err(PyErr::new::<PyRuntimeError, _>(format!(
                "未知的 matcher 类型: {}",
                other
            )))
        }
    })
}

fn parse_send_pattern(v: &Bound<PyAny>) -> PyResult<SendPattern> {
    let dict = v.cast::<PyDict>()?;
    let messages_any = dict
        .get_item("messages")?
        .ok_or_else(|| PyErr::new::<PyRuntimeError, _>("send_pattern.messages 不能为空".to_string()))?;
    let messages = parse_message_template_list(&messages_any)?;
    let rate_per_sec: Option<f64> = match dict.get_item("rate_per_sec")? {
        Some(v) if !v.is_none() => Some(v.extract()?),
        _ => None,
    };
    let interval_ms: Option<u64> = match dict.get_item("interval_ms")? {
        Some(v) if !v.is_none() => Some(v.extract()?),
        _ => None,
    };
    let iterate_data_pool: bool = match dict.get_item("iterate_data_pool")? {
        Some(v) => v.extract().unwrap_or(false),
        None => false,
    };
    Ok(SendPattern {
        messages,
        rate_per_sec,
        interval_ms,
        iterate_data_pool,
    })
}

fn parse_heartbeat(v: &Bound<PyAny>) -> PyResult<Heartbeat> {
    let dict = v.cast::<PyDict>()?;
    let interval_secs: u64 = dict
        .get_item("interval_secs")?
        .ok_or_else(|| PyErr::new::<PyRuntimeError, _>("heartbeat.interval_secs 不能为空".to_string()))?
        .extract()?;
    let timeout_secs: u64 = dict
        .get_item("timeout_secs")?
        .ok_or_else(|| PyErr::new::<PyRuntimeError, _>("heartbeat.timeout_secs 不能为空".to_string()))?
        .extract()?;
    let payload = match dict.get_item("payload")? {
        Some(p) if !p.is_none() => Some(parse_message_template(&p)?),
        _ => None,
    };
    Ok(Heartbeat {
        interval_secs,
        payload,
        timeout_secs,
    })
}

fn parse_reconnect(v: &Bound<PyAny>) -> PyResult<ReconnectPolicy> {
    let dict = v.cast::<PyDict>()?;
    let max_attempts: u32 = dict
        .get_item("max_attempts")?
        .ok_or_else(|| {
            PyErr::new::<PyRuntimeError, _>("reconnect.max_attempts 不能为空".to_string())
        })?
        .extract()?;
    let backoff_ms: u64 = dict
        .get_item("backoff_ms")?
        .ok_or_else(|| {
            PyErr::new::<PyRuntimeError, _>("reconnect.backoff_ms 不能为空".to_string())
        })?
        .extract()?;
    Ok(ReconnectPolicy {
        max_attempts,
        backoff_ms,
    })
}

fn parse_assert_options(v: &Bound<PyAny>) -> PyResult<Vec<WsAssertOption>> {
    let list = v.cast::<PyList>()?;
    let mut out = Vec::with_capacity(list.len());
    for item in list.iter() {
        let dict = item.cast::<PyDict>()?;
        let jsonpath: String = dict
            .get_item("jsonpath")?
            .ok_or_else(|| {
                PyErr::new::<PyRuntimeError, _>("ws_assert.jsonpath 不能为空".to_string())
            })?
            .extract()?;
        let reference_object_any = dict.get_item("reference_object")?.ok_or_else(|| {
            PyErr::new::<PyRuntimeError, _>("ws_assert.reference_object 不能为空".to_string())
        })?;
        let reference_object: Value = depythonize(&reference_object_any)?;
        let on_match: AssertTrigger = match dict.get_item("on_match")? {
            Some(s) if !s.is_none() => {
                let s: String = s.extract()?;
                match s.as_str() {
                    "EveryMessage" => AssertTrigger::EveryMessage,
                    "FirstMatch" => AssertTrigger::FirstMatch,
                    "NoMatch" => AssertTrigger::NoMatch,
                    other => {
                        return Err(PyErr::new::<PyRuntimeError, _>(format!(
                            "未知的 on_match: {}",
                            other
                        )))
                    }
                }
            }
            _ => AssertTrigger::EveryMessage,
        };
        out.push(WsAssertOption {
            jsonpath,
            reference_object,
            on_match,
        });
    }
    Ok(out)
}

pub fn new(py: Python<'_>, ws_endpoints: Py<PyList>) -> PyResult<Vec<WsEndpoint>> {
    let bound = ws_endpoints.bind(py);
    let mut out: Vec<WsEndpoint> = Vec::new();
    for item in bound.iter() {
        let dict = item.cast::<PyDict>()?;
        let name: String = dict
            .get_item("name")?
            .ok_or_else(|| PyErr::new::<PyRuntimeError, _>("ws endpoint.name 不能为空".to_string()))?
            .extract()?;
        let url: String = dict
            .get_item("url")?
            .ok_or_else(|| PyErr::new::<PyRuntimeError, _>("ws endpoint.url 不能为空".to_string()))?
            .extract()?;
        let weight: u32 = dict
            .get_item("weight")?
            .ok_or_else(|| {
                PyErr::new::<PyRuntimeError, _>("ws endpoint.weight 不能为空".to_string())
            })?
            .extract()?;
        let mode_any = dict
            .get_item("mode")?
            .ok_or_else(|| PyErr::new::<PyRuntimeError, _>("ws endpoint.mode 不能为空".to_string()))?;
        let mode = parse_mode(&mode_any)?;
        let subprotocols: Option<Vec<String>> = dict
            .get_item("subprotocols")?
            .filter(|v| !v.is_none())
            .map(|v| v.extract::<Vec<String>>())
            .transpose()?;
        let headers: Option<HashMap<String, String>> = dict
            .get_item("headers")?
            .filter(|v| !v.is_none())
            .map(|v| depythonize(&v))
            .transpose()?;
        let on_connect = dict
            .get_item("on_connect")?
            .filter(|v| !v.is_none())
            .map(|v| parse_message_template_list(&v))
            .transpose()?;
        let send_pattern = dict
            .get_item("send_pattern")?
            .filter(|v| !v.is_none())
            .map(|v| parse_send_pattern(&v))
            .transpose()?;
        let heartbeat = dict
            .get_item("heartbeat")?
            .filter(|v| !v.is_none())
            .map(|v| parse_heartbeat(&v))
            .transpose()?;
        let connection_ttl_secs: Option<u64> = dict
            .get_item("connection_ttl_secs")?
            .filter(|v| !v.is_none())
            .map(|v| v.extract::<u64>())
            .transpose()?;
        let reconnect = dict
            .get_item("reconnect")?
            .filter(|v| !v.is_none())
            .map(|v| parse_reconnect(&v))
            .transpose()?;
        let assert_options = dict
            .get_item("assert_options")?
            .filter(|v| !v.is_none())
            .map(|v| parse_assert_options(&v))
            .transpose()?;
        let think_time_option: Option<ThinkTime> = dict
            .get_item("think_time_option")?
            .filter(|v| !v.is_none())
            .map(|v| depythonize(&v))
            .transpose()?;

        let setup_options_py = match dict.get_item("setup_options")? {
            Some(v) if !v.is_none() => Some(v.extract::<Py<PyList>>()?),
            _ => None,
        };
        let setup_options = utils::parse_setup_options::new(py, setup_options_py)?;

        let teardown_options_py = match dict.get_item("teardown_options")? {
            Some(v) if !v.is_none() => Some(v.extract::<Py<PyList>>()?),
            _ => None,
        };
        let teardown_options = utils::parse_setup_options::new(py, teardown_options_py)?;

        out.push(WsEndpoint {
            name,
            url,
            weight,
            subprotocols,
            headers,
            mode,
            on_connect,
            send_pattern,
            heartbeat,
            connection_ttl_secs,
            reconnect,
            assert_options,
            think_time_option,
            setup_options,
            teardown_options,
        });
    }
    Ok(out)
}
