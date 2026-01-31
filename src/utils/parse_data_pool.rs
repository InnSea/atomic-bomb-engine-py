use atomic_bomb_engine::models::data_pool::{DataPool, DataPoolMode};
use pyo3::types::{PyAnyMethods, PyDict, PyDictMethods};
use pyo3::{Py, Python};
use std::sync::Arc;

pub fn new(
    py: Python,
    data_pool_option: Option<Py<PyDict>>,
) -> anyhow::Result<Option<Arc<DataPool>>> {
    match data_pool_option {
        Some(opt) => {
            let dict = opt.bind(py);
            
            // 获取文件路径，如果为 None 或空字符串则跳过数据池
            let file_path: Option<String> = match dict.get_item("file_path")? {
                Some(v) => {
                    if v.is_none() {
                        None
                    } else {
                        match v.extract::<String>() {
                            Ok(s) if !s.is_empty() => Some(s),
                            _ => None,
                        }
                    }
                }
                None => None,
            };
            
            // 如果没有有效的文件路径，返回 None（不使用数据池）
            let file_path = match file_path {
                Some(p) => p,
                None => return Ok(None),
            };
            
            // 获取模式
            let mode_str: String = match dict.get_item("mode")? {
                Some(v) => {
                    if v.is_none() {
                        "sequential".to_string()
                    } else {
                        v.extract().unwrap_or_else(|_| "sequential".to_string())
                    }
                }
                None => "sequential".to_string(),
            };
            let mode = DataPoolMode::from(mode_str.as_str());
            
            // 创建数据池
            match DataPool::from_file(&file_path, mode) {
                Ok(pool) => Ok(Some(Arc::new(pool))),
                Err(e) => Err(anyhow::Error::msg(format!("创建数据池失败: {}", e))),
            }
        }
        None => Ok(None),
    }
}
