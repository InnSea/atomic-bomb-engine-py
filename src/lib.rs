use pyo3::prelude::*;
use pyo3::types::PyModule;
mod py_lib;
mod utils;

#[pymodule]
#[pyo3(name = "atomic_bomb_engine")]
fn atomic_bomb_engine(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(
        py_lib::assert_option_func::assert_option,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_lib::endpoint_func::endpoint, m)?)?;
    m.add_function(wrap_pyfunction!(py_lib::step_option_func::step_option, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_lib::setup_option_func::setup_option,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_lib::jsonpath_extract_func::jsonpath_extract_option,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_lib::think_time_option_func::think_time_option,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_lib::multipart_option_func::multipart_option,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_lib::data_pool_func::data_pool_option,
        m
    )?)?;
    m.add_class::<py_lib::batch_runner::BatchRunner>()?;

    // ========== WebSocket 注册 ==========
    m.add_function(wrap_pyfunction!(py_lib::ws_endpoint_func::ws_endpoint, m)?)?;
    m.add_function(wrap_pyfunction!(py_lib::ws_message_func::ws_text, m)?)?;
    m.add_function(wrap_pyfunction!(py_lib::ws_message_func::ws_binary, m)?)?;
    m.add_function(wrap_pyfunction!(py_lib::ws_message_func::ws_json, m)?)?;
    m.add_function(wrap_pyfunction!(py_lib::ws_match_func::ws_match_sequential, m)?)?;
    m.add_function(wrap_pyfunction!(py_lib::ws_match_func::ws_match_jsonpath, m)?)?;
    m.add_function(wrap_pyfunction!(py_lib::ws_match_func::ws_mode_oneway, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_lib::ws_match_func::ws_mode_request_response,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_lib::ws_options_func::ws_send_pattern, m)?)?;
    m.add_function(wrap_pyfunction!(py_lib::ws_options_func::ws_heartbeat, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_lib::ws_options_func::ws_reconnect_policy,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_lib::ws_options_func::ws_assert_option,
        m
    )?)?;
    m.add_class::<py_lib::ws_batch_runner::WsBatchRunner>()?;
    Ok(())
}