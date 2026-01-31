use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering};
use rand::Rng;

/// 数据池读取模式
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum DataPoolMode {
    /// 随机读取
    Random,
    /// 循环顺序读取
    Sequential,
}

impl Default for DataPoolMode {
    fn default() -> Self {
        DataPoolMode::Sequential
    }
}

impl From<&str> for DataPoolMode {
    fn from(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "random" => DataPoolMode::Random,
            "sequential" | "sequence" | "loop" => DataPoolMode::Sequential,
            _ => DataPoolMode::Sequential,
        }
    }
}

/// 数据池配置（用于从Python传入）
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DataPoolConfig {
    /// CSV文件路径
    pub file_path: String,
    /// 读取模式
    pub mode: DataPoolMode,
}

/// 数据池统计信息
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct DataPoolStats {
    /// 总行数
    pub total_rows: usize,
    /// 读取模式
    pub mode: String,
    /// 循环次数（仅循环模式有意义）
    pub cycles: usize,
}

/// 数据池核心结构
pub struct DataPool {
    /// 字段名（CSV第一行）
    pub headers: Vec<String>,
    /// 数据行
    pub rows: Vec<Vec<String>>,
    /// 读取模式
    pub mode: DataPoolMode,
    /// 当前索引（循环模式使用）
    current_index: AtomicUsize,
    /// 循环次数
    cycles: AtomicUsize,
}

impl DataPool {
    /// 从CSV内容创建数据池
    pub fn from_csv_content(content: &str, mode: DataPoolMode) -> Result<Self, String> {
        let mut lines = content.lines();
        
        // 解析表头
        let header_line = lines.next().ok_or("CSV文件为空")?;
        let headers: Vec<String> = header_line
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();
        
        if headers.is_empty() {
            return Err("CSV文件没有字段名".to_string());
        }
        
        // 解析数据行
        let mut rows: Vec<Vec<String>> = Vec::new();
        for (line_num, line) in lines.enumerate() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            
            let values: Vec<String> = line
                .split(',')
                .map(|s| s.trim().to_string())
                .collect();
            
            if values.len() != headers.len() {
                return Err(format!(
                    "第{}行的列数({})与表头列数({})不匹配",
                    line_num + 2,
                    values.len(),
                    headers.len()
                ));
            }
            
            rows.push(values);
        }
        
        if rows.is_empty() {
            return Err("CSV文件没有数据行".to_string());
        }
        
        Ok(Self {
            headers,
            rows,
            mode,
            current_index: AtomicUsize::new(0),
            cycles: AtomicUsize::new(0),
        })
    }
    
    /// 从CSV文件路径创建数据池
    pub fn from_file(file_path: &str, mode: DataPoolMode) -> Result<Self, String> {
        let content = std::fs::read_to_string(file_path)
            .map_err(|e| format!("读取CSV文件失败: {}", e))?;
        Self::from_csv_content(&content, mode)
    }
    
    /// 获取下一行数据（根据模式）
    /// 返回一个包含 (字段名, 值) 的Vec
    pub fn get_next_row(&self) -> Vec<(String, String)> {
        let row_index = match self.mode {
            DataPoolMode::Random => {
                let mut rng = rand::thread_rng();
                rng.gen_range(0..self.rows.len())
            }
            DataPoolMode::Sequential => {
                let index = self.current_index.fetch_add(1, Ordering::Relaxed);
                let actual_index = index % self.rows.len();
                
                // 检查是否完成一次循环
                if actual_index == 0 && index > 0 {
                    self.cycles.fetch_add(1, Ordering::Relaxed);
                }
                
                actual_index
            }
        };
        
        self.headers
            .iter()
            .zip(self.rows[row_index].iter())
            .map(|(h, v)| (h.clone(), v.clone()))
            .collect()
    }
    
    /// 获取总行数
    pub fn total_rows(&self) -> usize {
        self.rows.len()
    }
    
    /// 获取循环次数
    pub fn get_cycles(&self) -> usize {
        self.cycles.load(Ordering::Relaxed)
    }
    
    /// 获取统计信息
    pub fn get_stats(&self) -> DataPoolStats {
        DataPoolStats {
            total_rows: self.rows.len(),
            mode: match self.mode {
                DataPoolMode::Random => "random".to_string(),
                DataPoolMode::Sequential => "sequential".to_string(),
            },
            cycles: self.get_cycles(),
        }
    }
}

// 实现 Clone（手动实现因为 AtomicUsize 不支持 Clone）
impl Clone for DataPool {
    fn clone(&self) -> Self {
        Self {
            headers: self.headers.clone(),
            rows: self.rows.clone(),
            mode: self.mode.clone(),
            current_index: AtomicUsize::new(self.current_index.load(Ordering::Relaxed)),
            cycles: AtomicUsize::new(self.cycles.load(Ordering::Relaxed)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_parse_csv() {
        let csv_content = "username,password,age\nuser1,pass1,20\nuser2,pass2,25\nuser3,pass3,30";
        let pool = DataPool::from_csv_content(csv_content, DataPoolMode::Sequential).unwrap();
        
        assert_eq!(pool.headers, vec!["username", "password", "age"]);
        assert_eq!(pool.rows.len(), 3);
        assert_eq!(pool.rows[0], vec!["user1", "pass1", "20"]);
    }
    
    #[test]
    fn test_sequential_mode() {
        let csv_content = "name\na\nb\nc";
        let pool = DataPool::from_csv_content(csv_content, DataPoolMode::Sequential).unwrap();
        
        let row1 = pool.get_next_row();
        assert_eq!(row1[0].1, "a");
        
        let row2 = pool.get_next_row();
        assert_eq!(row2[0].1, "b");
        
        let row3 = pool.get_next_row();
        assert_eq!(row3[0].1, "c");
        
        // 循环回到第一行
        let row4 = pool.get_next_row();
        assert_eq!(row4[0].1, "a");
    }
    
    #[test]
    fn test_random_mode() {
        let csv_content = "name\na\nb\nc";
        let pool = DataPool::from_csv_content(csv_content, DataPoolMode::Random).unwrap();
        
        // 随机模式下获取的值应该是有效的
        for _ in 0..10 {
            let row = pool.get_next_row();
            assert!(["a", "b", "c"].contains(&row[0].1.as_str()));
        }
    }
}
