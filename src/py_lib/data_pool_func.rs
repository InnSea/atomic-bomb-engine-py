use pyo3::types::{PyAny, PyDict, PyDictMethods};
use pyo3::{pyfunction, Py, PyResult, Python};

#[pyfunction]
#[pyo3(signature=(file_path, mode="sequential"))]
pub(crate) fn data_pool_option(
    py: Python,
    file_path: String,
    mode: &str,
) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    dict.set_item("file_path", file_path)?;
    dict.set_item("mode", mode)?;
    Ok(dict.into_any().unbind())
}
