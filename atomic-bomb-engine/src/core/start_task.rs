use crate::core::concurrency_controller::ConcurrencyController;
use crate::core::setup;
use crate::models::api_endpoint::ApiEndpoint;
use crate::models::api_endpoint_stats::ApiEndpointStats;
use crate::models::assert_error_stats::AssertErrorStats;
use crate::models::assert_task::AssertTask;
use crate::models::data_pool::DataPool;
use crate::models::http_error_stats::HttpErrorStats;
use anyhow::Error;
use futures::StreamExt;
use handlebars::Handlebars;
use histogram::AtomicHistogram;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, COOKIE};
use reqwest::{multipart, Client, Method, StatusCode};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::error::Error as std_error;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::mpsc::Sender;
use tokio::sync::Mutex;

pub(crate) async fn start_concurrency(
    client: Client,
    controller_arc: Arc<ConcurrencyController>,
    concurrent_number_arc: Arc<AtomicUsize>,
    extract_map_arc: Arc<BTreeMap<String, Value>>,
    endpoint_arc: Arc<ApiEndpoint>,
    api_stats: Arc<ApiEndpointStats>,
    total_requests_arc: Arc<AtomicUsize>,
    histogram_arc: Arc<AtomicHistogram>,
    max_response_time_arc: Arc<AtomicU64>,
    min_response_time_arc: Arc<AtomicU64>,
    total_response_size_arc: Arc<AtomicUsize>,
    total_response_time_ms_arc: Arc<AtomicU64>,
    successful_requests_arc: Arc<AtomicUsize>,
    err_count_arc: Arc<AtomicUsize>,
    http_errors_arc: Arc<Mutex<HttpErrorStats>>,
    assert_errors_arc: Arc<Mutex<AssertErrorStats>>,
    tx_assert: Sender<AssertTask>,
    test_start: Instant,
    test_end: Instant,
    is_need_render_template: bool,
    verbose: bool,
    should_stop: Arc<std::sync::atomic::AtomicBool>,
    data_pool: Option<Arc<DataPool>>,
    engine_errors: Arc<Mutex<Vec<String>>>,
) -> Result<(), Error> {
    fn atomic_max(atomic: &AtomicU64, val: u64) {
        let mut current = atomic.load(Ordering::Relaxed);
        while val > current {
            match atomic.compare_exchange_weak(current, val, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }
    fn atomic_min(atomic: &AtomicU64, val: u64) {
        let mut current = atomic.load(Ordering::Relaxed);
        while val < current {
            match atomic.compare_exchange_weak(current, val, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }
    // 避免在热路径反复读字段, 拿引用就好
    let _ = test_start; // 保留给未来使用, 同时避免 unused 警告

    let mut is_need_render = is_need_render_template;
    let semaphore = controller_arc.get_semaphore();
    let _permit = semaphore.acquire().await.expect("获取信号量许可失败");

    // endpoint_arc: Arc<ApiEndpoint> 只读, 直接按字段取
    let api_name_clone = endpoint_arc.name.clone();
    let method_clone = endpoint_arc.method.clone();
    let json_obj_base = endpoint_arc.json.clone();
    let form_data_base = endpoint_arc.form_data.clone();
    let multipart_base = endpoint_arc.multipart_options.clone();
    let headers_base = endpoint_arc.headers.clone();
    let cookie_base = endpoint_arc.cookies.clone();
    let assert_options_base = endpoint_arc.assert_options.clone();
    let think_time_base = endpoint_arc.think_time_option.clone();
    let api_setup_base = endpoint_arc.setup_options.clone();
    let api_teardown_base = endpoint_arc.teardown_options.clone();
    let endpoint_url = endpoint_arc.url.clone();

    // 统计并发数(累计计数, 与原行为一致)
    api_stats.concurrent_number.fetch_add(1, Ordering::Relaxed);
    concurrent_number_arc.fetch_add(1, Ordering::Relaxed);
    // 复用 Handlebars 实例
    let handlebars = Handlebars::new();
    // 在到达结束时间后停止发送请求
    'RETRY: while Instant::now() < test_end && !should_stop.load(Ordering::SeqCst) {
        let mut api_extract_b_tree_map = BTreeMap::new();
        api_extract_b_tree_map.extend((*extract_map_arc).clone());
        if let Some(ref pool) = data_pool {
            let row_data = pool.get_next_row();
            for (key, value) in row_data {
                api_extract_b_tree_map.insert(key, Value::String(value));
            }
        }
        let api_setup_clone = api_setup_base.clone();
        if let Some(setup_options) = api_setup_clone {
            is_need_render = true;
            match setup::start_setup(
                setup_options,
                api_extract_b_tree_map.clone(),
                client.clone(),
            )
            .await
            {
                Ok(res) => {
                    if let Some(extract) = res {
                        api_extract_b_tree_map.extend(extract);
                    };
                }
                Err(e) => {
                    let err_msg = format!(
                        "接口-{:?}初始化失败,1秒后重试!!: {:?}",
                        api_name_clone.clone(),
                        e.to_string()
                    );
                    engine_errors.lock().await.push(err_msg);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue 'RETRY;
                }
            };
        }
        let json_obj_clone = json_obj_base.clone();
        let form_data_clone = form_data_base.clone();
        let multipart_clone = multipart_base.clone();
        let headers_clone = headers_base.clone();
        let cookie_clone = cookie_base.clone();
        let assert_options_clone = assert_options_base.clone();
        let think_time_clone = think_time_base.clone();
        let method = match Method::from_str(&method_clone.to_uppercase()) {
            Ok(m) => m,
            Err(e) => {
                return Err(Error::msg(format!("构建请求方法失败:{:?}", e.to_string())));
            }
        };
        let mut request = client.request(method, endpoint_url.clone());
        let mut headers = HeaderMap::new();
        if let Some(headers_map) = headers_clone {
            headers.extend(headers_map.iter().map(|(k, v)| {
                let header_name = k.parse::<HeaderName>().expect("无效的header名称");
                match is_need_render_template {
                    true => {
                        let new_val =
                            match handlebars.render_template(v, &json!(api_extract_b_tree_map)) {
                                Ok(v) => v,
                                Err(_) => v.to_string(),
                            };
                        let header_value = new_val.parse::<HeaderValue>().expect("无效的header值");
                        (header_name.clone(), header_value)
                    }
                    false => {
                        let header_value = v.parse::<HeaderValue>().expect("无效的header值");
                        (header_name, header_value)
                    }
                }
            }));
        }
        if let Some(ref source) = cookie_clone {
            let cookie_val = match is_need_render_template {
                true => match handlebars.render_template(source, &json!(api_extract_b_tree_map)) {
                    Ok(c) => c,
                    Err(_) => source.to_string(),
                },
                false => source.to_string(),
            };
            match HeaderValue::from_str(&cookie_val) {
                Ok(h) => {
                    headers.insert(COOKIE, h);
                }
                Err(e) => return Err(Error::msg(format!("设置cookie失败:{:?}", e))),
            }
        }
        request = request.headers(headers);
        if let Some(json_value) = json_obj_clone {
            let json_source = if json_value.is_string() {
                json_value.as_str().unwrap().to_string()
            } else {
                json_value.to_string()
            };

            let json_val = match is_need_render_template {
                true => {
                    let json_string = match handlebars
                        .render_template(&json_source, &json!(api_extract_b_tree_map))
                    {
                        Ok(j) => j,
                        Err(_) => json_source.clone(),
                    };
                    match Value::from_str(&json_string) {
                        Ok(val) => val,
                        Err(e) => {
                            return Err(Error::msg(format!(
                                "转换json失败:{:?}, 原始json: {:?}",
                                e, json_string
                            )))
                        }
                    }
                }
                false => {
                    if json_value.is_string() {
                        match Value::from_str(&json_source) {
                            Ok(val) => val,
                            Err(e) => {
                                return Err(Error::msg(format!(
                                    "转换json失败:{:?}, 原始json: {:?}",
                                    e, json_source
                                )))
                            }
                        }
                    } else {
                        json_value
                    }
                }
            };
            if verbose {
                println!("json:{:?}", json_val);
            };
            request = request.json(&json_val);
        }
        if let Some(mut form_data) = form_data_clone {
            if is_need_render {
                form_data.iter_mut().for_each(|(_key, value)| {
                    let new_val =
                        match handlebars.render_template(value, &json!(api_extract_b_tree_map)) {
                            Ok(v) => v,
                            Err(_) => value.to_string(),
                        };
                    *value = new_val;
                })
            };
            request = request.form(&form_data);
        };
        if let Some(multipart_options) = multipart_clone {
            let mut multipart_form = multipart::Form::new();
            for mo in multipart_options {
                let file = match tokio::fs::File::open(mo.path).await {
                    Ok(f) => f,
                    Err(e) => {
                        return Err(Error::msg(format!("打开文件出错::{:?}", e.to_string())));
                    }
                };
                let part = match multipart::Part::stream(file)
                    .file_name(mo.file_name)
                    .mime_str(&*mo.mime)
                {
                    Ok(p) => p,
                    Err(e) => {
                        return Err(Error::msg(format!("构建part失败::{:?}", e.to_string())));
                    }
                };
                multipart_form = multipart_form.part(mo.form_key, part);
            }
            request = request.multipart(multipart_form);
        };
        if verbose {
            println!("{:?}", request);
        };
        if let Some(think_time) = think_time_clone {
            match think_time.min_millis <= think_time.max_millis {
                true => {
                    let mut rng = StdRng::from_entropy();
                    let tt = rng.gen_range(think_time.min_millis..=think_time.max_millis);
                    if verbose {
                        println!("思考时间：{:?}", tt);
                    }
                    tokio::time::sleep(Duration::from_millis(tt)).await;
                }
                false => {
                    let err_msg = "最小思考时间大于最大思考时间，该配置不生效!".to_string();
                    engine_errors.lock().await.push(err_msg);
                }
            }
        }
        let start = Instant::now();
        match request.send().await {
            Ok(response) => {
                total_requests_arc.fetch_add(1, Ordering::Relaxed);
                api_stats.total_requests.fetch_add(1, Ordering::Relaxed);
                let status = response.status();
                let url_parse = response.url().clone();
                match status {
                    StatusCode::OK
                    | StatusCode::CREATED
                    | StatusCode::ACCEPTED
                    | StatusCode::NON_AUTHORITATIVE_INFORMATION
                    | StatusCode::NO_CONTENT
                    | StatusCode::RESET_CONTENT
                    | StatusCode::PARTIAL_CONTENT
                    | StatusCode::MULTI_STATUS
                    | StatusCode::ALREADY_REPORTED
                    | StatusCode::IM_USED
                    | StatusCode::MULTIPLE_CHOICES
                    | StatusCode::MOVED_PERMANENTLY
                    | StatusCode::FOUND
                    | StatusCode::SEE_OTHER
                    | StatusCode::NOT_MODIFIED
                    | StatusCode::USE_PROXY
                    | StatusCode::TEMPORARY_REDIRECT
                    | StatusCode::PERMANENT_REDIRECT => {
                        let duration = start.elapsed().as_millis() as u64;
                        total_response_time_ms_arc.fetch_add(duration, Ordering::Relaxed);
                        api_stats
                            .total_response_time_ms
                            .fetch_add(duration, Ordering::Relaxed);
                        atomic_max(&max_response_time_arc, duration);
                        atomic_max(&api_stats.max_response_time, duration);
                        atomic_min(&min_response_time_arc, duration);
                        atomic_min(&api_stats.min_response_time, duration);
                        // AtomicHistogram: 无锁原子 increment
                        let _ = api_stats.histogram.increment(duration);
                        let _ = histogram_arc.increment(duration);
                        let resp_headers = response.headers();
                        let headers_size = resp_headers.iter().fold(0, |acc, (name, value)| {
                            acc + name.as_str().len() + 2 + value.as_bytes().len() + 2
                        });
                        total_response_size_arc.fetch_add(headers_size, Ordering::Relaxed);
                        api_stats
                            .total_response_size
                            .fetch_add(headers_size, Ordering::Relaxed);
                        // 响应流 - 完全无锁
                        let mut stream = response.bytes_stream();
                        let need_body = assert_options_clone.is_some() || verbose;
                        let mut body_bytes = Vec::new();
                        let mut stream_error = false;
                        while let Some(item) = stream.next().await {
                            match item {
                                Ok(chunk) => {
                                    total_response_size_arc
                                        .fetch_add(chunk.len(), Ordering::Relaxed);
                                    api_stats
                                        .total_response_size
                                        .fetch_add(chunk.len(), Ordering::Relaxed);
                                    if need_body {
                                        body_bytes.extend_from_slice(&chunk);
                                    }
                                }
                                Err(e) => {
                                    stream_error = true;
                                    api_stats.err_count.fetch_add(1, Ordering::Relaxed);
                                    err_count_arc.fetch_add(1, Ordering::Relaxed);
                                    http_errors_arc
                                        .lock()
                                        .await
                                        .increment(
                                            api_name_clone.clone(),
                                            e.url(),
                                            match e.status() {
                                                None => 0u16,
                                                Some(status_code) => status_code.as_u16(),
                                            },
                                            e.to_string(),
                                            match e.source() {
                                                None => "-".to_string(),
                                                Some(source) => source.to_string(),
                                            },
                                        )
                                        .await;
                                    break;
                                }
                            };
                        }
                        if verbose && !body_bytes.is_empty() {
                            let buffer = String::from_utf8(body_bytes.clone())
                                .expect("无法转换响应体为字符串");
                            println!("{:+?}", buffer);
                        }
                        // 断言 - 异步 fire-and-forget, 不再 oneshot 等待
                        if !stream_error {
                            match assert_options_clone {
                                Some(assert_options) => {
                                    if body_bytes.len() > 0 {
                                        let task = AssertTask {
                                            assert_options: assert_options.clone(),
                                            body_bytes,
                                            verbose,
                                            err_count: err_count_arc.clone(),
                                            api_err_count: api_stats.err_count.clone(),
                                            assert_errors: assert_errors_arc.clone(),
                                            endpoint: endpoint_arc.clone(),
                                            api_name: api_name_clone.clone(),
                                            successful_requests: successful_requests_arc.clone(),
                                            api_successful_requests: api_stats
                                                .successful_requests
                                                .clone(),
                                        };
                                        tx_assert.send(task).await.expect("生产断言任务失败");
                                    };
                                }
                                None => {
                                    successful_requests_arc.fetch_add(1, Ordering::Relaxed);
                                    api_stats
                                        .successful_requests
                                        .fetch_add(1, Ordering::Relaxed);
                                }
                            };
                        }
                    }
                    // 状态码错误
                    _ => {
                        let duration = start.elapsed().as_millis() as u64;
                        total_response_time_ms_arc.fetch_add(duration, Ordering::Relaxed);
                        api_stats
                            .total_response_time_ms
                            .fetch_add(duration, Ordering::Relaxed);
                        err_count_arc.fetch_add(1, Ordering::Relaxed);
                        api_stats.err_count.fetch_add(1, Ordering::Relaxed);
                        let status_code = u16::from(response.status());
                        atomic_max(&max_response_time_arc, duration);
                        atomic_max(&api_stats.max_response_time, duration);
                        atomic_min(&min_response_time_arc, duration);
                        atomic_min(&api_stats.min_response_time, duration);
                        // AtomicHistogram: 无锁原子 increment
                        let _ = api_stats.histogram.increment(duration);
                        let _ = histogram_arc.increment(duration);
                        let resp_headers = response.headers();
                        let headers_size = resp_headers.iter().fold(0, |acc, (name, value)| {
                            acc + name.as_str().len() + 2 + value.as_bytes().len() + 2
                        });
                        total_response_size_arc.fetch_add(headers_size, Ordering::Relaxed);
                        api_stats
                            .total_response_size
                            .fetch_add(headers_size, Ordering::Relaxed);
                        let mut stream = response.bytes_stream();
                        let mut body_bytes = Vec::new();
                        while let Some(item) = stream.next().await {
                            match item {
                                Ok(chunk) => {
                                    total_response_size_arc
                                        .fetch_add(chunk.len(), Ordering::Relaxed);
                                    api_stats
                                        .total_response_size
                                        .fetch_add(chunk.len(), Ordering::Relaxed);
                                    body_bytes.extend_from_slice(&chunk);
                                }
                                Err(e) => {
                                    api_stats.err_count.fetch_add(1, Ordering::Relaxed);
                                    err_count_arc.fetch_add(1, Ordering::Relaxed);
                                    http_errors_arc
                                        .lock()
                                        .await
                                        .increment(
                                            api_name_clone.clone(),
                                            e.url(),
                                            match e.status() {
                                                None => 0u16,
                                                Some(status_code) => status_code.as_u16(),
                                            },
                                            e.to_string(),
                                            match e.source() {
                                                None => "-".to_string(),
                                                Some(source) => source.to_string(),
                                            },
                                        )
                                        .await;
                                    break;
                                }
                            };
                        }
                        let body_bytes_clone = body_bytes.clone();
                        let buffer =
                            String::from_utf8(body_bytes_clone).expect("无法转换响应体为字符串");
                        let err_msg =
                            format!("HTTP 错误: 状态码 {:?}, body:{:?}", status_code, buffer);
                        http_errors_arc
                            .lock()
                            .await
                            .increment(
                                api_name_clone.clone(),
                                Some(&url_parse),
                                status.as_u16(),
                                err_msg,
                                "-".to_string(),
                            )
                            .await;
                        if verbose {
                            println!(
                                "{:?}-HTTP 错误: 状态码 {:?}, 响应体：{:?}",
                                api_name_clone.clone(),
                                status_code,
                                buffer
                            )
                        }
                    }
                }
            }
            Err(e) => {
                total_requests_arc.fetch_add(1, Ordering::Relaxed);
                api_stats.total_requests.fetch_add(1, Ordering::Relaxed);
                err_count_arc.fetch_add(1, Ordering::Relaxed);
                api_stats.err_count.fetch_add(1, Ordering::Relaxed);
                let status_code: u16 = match e.status() {
                    None => 0,
                    Some(code) => u16::from(code),
                };

                if verbose {
                    eprintln!("{:#?}", e.to_string());
                };

                let err_source = match e.source() {
                    None => "None".to_string(),
                    Some(source) => source.to_string(),
                };
                http_errors_arc
                    .lock()
                    .await
                    .increment(
                        api_name_clone.clone(),
                        e.url(),
                        status_code,
                        e.to_string(),
                        err_source,
                    )
                    .await;
            }
        }
        if let Some(ref teardown_opts) = api_teardown_base {
            match setup::start_setup(
                teardown_opts.clone(),
                api_extract_b_tree_map.clone(),
                client.clone(),
            )
            .await
            {
                Ok(_) => {}
                Err(e) => {
                    let err_msg = format!(
                        "接口-{:?} teardown执行失败: {:?}",
                        api_name_clone.clone(),
                        e.to_string()
                    );
                    engine_errors.lock().await.push(err_msg);
                }
            }
        }
    }
    Ok(())
}
