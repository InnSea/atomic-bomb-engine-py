use pyo3::types::{PyAnyMethods, PyDict, PyDictMethods};
use pyo3::{Py, Python};
use serde_json::Value;
use std::collections::BTreeMap;

pub fn new(
    py: Python,
    global_variables: Option<Py<PyDict>>,
) -> anyhow::Result<Option<BTreeMap<String, Value>>> {
    match global_variables {
        Some(opt) => {
            let dict = opt.bind(py);
            let mut vars: BTreeMap<String, Value> = BTreeMap::new();

            for (key, value) in dict.iter() {
                let k: String = key.extract()
                    .map_err(|e| anyhow::Error::msg(format!("全局变量key必须是字符串: {}", e)))?;

                // 根据Python类型转换为对应的JSON类型
                let v = if let Ok(s) = value.extract::<String>() {
                    Value::String(s)
                } else if let Ok(i) = value.extract::<i64>() {
                    Value::Number(i.into())
                } else if let Ok(f) = value.extract::<f64>() {
                    match serde_json::Number::from_f64(f) {
                        Some(n) => Value::Number(n),
                        None => Value::String(f.to_string()),
                    }
                } else if let Ok(b) = value.extract::<bool>() {
                    Value::Bool(b)
                } else if value.is_none() {
                    Value::Null
                } else {
                    // 其他类型转为字符串
                    Value::String(value.str()?.to_string())
                };

                vars.insert(k, v);
            }

            match vars.is_empty() {
                true => Ok(None),
                false => Ok(Some(vars)),
            }
        }
        None => Ok(None),
    }
}
