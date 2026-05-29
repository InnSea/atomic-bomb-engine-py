//! 将 WsBatchResult 转为 Python dict
use atomic_bomb_engine::models::ws_result::{WsApiResult, WsBatchResult};
use pyo3::types::{PyDict, PyDictMethods, PyList};
use pyo3::{Py, PyAny, PyResult, Python};

use crate::utils;

fn ws_api_result_to_dict(py: Python<'_>, r: &WsApiResult) -> PyResult<Py<PyDict>> {
    let d = PyDict::new(py);
    d.set_item("name", &r.name)?;
    d.set_item("url", &r.url)?;
    d.set_item("host", &r.host)?;
    d.set_item("path", &r.path)?;
    d.set_item("connections_opened", r.connections_opened)?;
    d.set_item("connections_active", r.connections_active)?;
    d.set_item("connections_failed", r.connections_failed)?;
    d.set_item("connections_dropped", r.connections_dropped)?;
    d.set_item("heartbeat_timeouts", r.heartbeat_timeouts)?;
    d.set_item("reconnects", r.reconnects)?;
    d.set_item("handshake_p50_ms", r.handshake_p50_ms)?;
    d.set_item("handshake_p95_ms", r.handshake_p95_ms)?;
    d.set_item("handshake_p99_ms", r.handshake_p99_ms)?;
    d.set_item("total_requests", r.total_requests)?;
    d.set_item("messages_received", r.messages_received)?;
    d.set_item("successful_requests", r.successful_requests)?;
    d.set_item("success_rate", r.success_rate)?;
    d.set_item("error_rate", r.error_rate)?;
    d.set_item("err_count", r.err_count)?;
    d.set_item("rps", r.rps)?;
    d.set_item("bytes_sent_kb", r.bytes_sent_kb)?;
    d.set_item("bytes_received_kb", r.bytes_received_kb)?;
    d.set_item("throughput_per_second_kb", r.throughput_per_second_kb)?;
    d.set_item("median_response_time", r.median_response_time)?;
    d.set_item("response_time_95", r.response_time_95)?;
    d.set_item("response_time_99", r.response_time_99)?;
    d.set_item("max_response_time", r.max_response_time)?;
    d.set_item("min_response_time", r.min_response_time)?;
    d.set_item("avg_response_time", r.avg_response_time)?;
    d.set_item("match_timeouts", r.match_timeouts)?;
    Ok(d.unbind())
}

fn ws_errors_to_list(
    py: Python<'_>,
    errors: &std::collections::HashMap<atomic_bomb_engine::models::ws_error_stats::WsErrKey, u32>,
) -> PyResult<Py<PyList>> {
    if errors.is_empty() {
        return Ok(PyList::empty(py).unbind());
    }
    let mut items: Vec<Py<PyDict>> = Vec::with_capacity(errors.len());
    for (k, count) in errors {
        let d = PyDict::new(py);
        d.set_item("name", &k.name)?;
        d.set_item("url", &k.url)?;
        d.set_item("host", &k.host)?;
        d.set_item("kind", format!("{:?}", k.kind))?;
        d.set_item("close_code", k.close_code)?;
        d.set_item("message", &k.msg)?;
        d.set_item("count", count)?;
        items.push(d.unbind());
    }
    Ok(PyList::new(py, items)?.unbind())
}

pub fn ws_batch_result_to_dict(py: Python<'_>, r: WsBatchResult) -> PyResult<Py<PyAny>> {
    let d = PyDict::new(py);
    d.set_item("total_duration", r.total_duration)?;
    d.set_item("timestamp", r.timestamp)?;

    d.set_item("total_connections_opened", r.total_connections_opened)?;
    d.set_item("total_connections_failed", r.total_connections_failed)?;
    d.set_item("total_connections_dropped", r.total_connections_dropped)?;
    d.set_item("total_concurrent_number", r.total_concurrent_number)?;
    d.set_item("total_reconnects", r.total_reconnects)?;
    d.set_item("total_heartbeat_timeouts", r.total_heartbeat_timeouts)?;
    d.set_item("handshake_p50_ms", r.handshake_p50_ms)?;
    d.set_item("handshake_p95_ms", r.handshake_p95_ms)?;
    d.set_item("handshake_p99_ms", r.handshake_p99_ms)?;

    d.set_item("total_messages_sent", r.total_messages_sent)?;
    d.set_item("total_messages_received", r.total_messages_received)?;
    d.set_item("successful_requests", r.successful_requests)?;
    d.set_item("success_rate", r.success_rate)?;
    d.set_item("error_rate", r.error_rate)?;
    d.set_item("err_count", r.err_count)?;
    d.set_item("errors_per_second", r.errors_per_second)?;
    d.set_item("rps", r.rps)?;

    d.set_item("total_data_sent_kb", r.total_data_sent_kb)?;
    d.set_item("total_data_received_kb", r.total_data_received_kb)?;
    d.set_item("throughput_per_second_kb", r.throughput_per_second_kb)?;

    d.set_item("median_response_time", r.median_response_time)?;
    d.set_item("response_time_95", r.response_time_95)?;
    d.set_item("response_time_99", r.response_time_99)?;
    d.set_item("max_response_time", r.max_response_time)?;
    d.set_item("min_response_time", r.min_response_time)?;
    d.set_item("avg_response_time", r.avg_response_time)?;
    d.set_item("total_match_timeouts", r.total_match_timeouts)?;

    let mut ws_results_list: Vec<Py<PyDict>> = Vec::with_capacity(r.ws_results.len());
    for item in r.ws_results.iter() {
        ws_results_list.push(ws_api_result_to_dict(py, item)?);
    }
    let ws_results = PyList::new(py, ws_results_list)?;
    d.set_item("ws_results", ws_results)?;

    let ws_errors = ws_errors_to_list(py, &r.ws_errors)?;
    d.set_item("ws_errors", ws_errors)?;

    let assert_errors =
        utils::create_assert_err_dict::create_assert_error_dict(py, &r.assert_errors)?;
    d.set_item("assert_errors", assert_errors)?;

    if let Some(ref dp) = r.data_pool_stats {
        let dp_dict = PyDict::new(py);
        dp_dict.set_item("total_rows", dp.total_rows)?;
        dp_dict.set_item("mode", &dp.mode)?;
        dp_dict.set_item("cycles", dp.cycles)?;
        d.set_item("data_pool_stats", dp_dict)?;
    }

    let engine_errors = PyList::new(py, &r.engine_errors)?;
    d.set_item("engine_errors", engine_errors)?;

    Ok(d.into_any().unbind())
}
