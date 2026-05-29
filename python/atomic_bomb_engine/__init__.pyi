from typing import Optional, List, Dict, Any

def assert_option(jsonpath: str, reference_object: any) -> Dict[str, Any]:
    """
    生成assert option
    :param jsonpath: jsonpath取值地址
    :param reference_object: 断言的值
    """

def step_option(increase_step: int, increase_interval: int) -> Dict[str, int]:
    """
    生成step option
    :param increase_step: 阶梯步长
    :param increase_interval: 阶梯间隔
    """

def think_time_option(min_millis: int, max_millis: int) -> Dict[str, int]:
    """
    思考时间选项
    :param min_millis:
    :param max_millis:
    :return:
    """

def endpoint(
    name: str,
    url: str,
    method: str,
    weight: int,
    json: Dict | str | None = None,
    form_data: Dict | None = None,
    multipart_options: List[Dict] | None = None,
    headers: Dict | None = None,
    cookies: str | None = None,
    assert_options: List | None = None,
    think_time_option: Dict[str, int] | None = None,
    setup_options: List | None = None,
    teardown_options: List | None = None,
) -> Dict[str, Any]:
    """
    生成endpoint
    :param assert_options:
    :param form_data:
    :param name: 接口名称
    :param url: 接口地址
    :param method: 请求方法
    :param weight 权重
    :param json: 请求json，支持两种格式：
        - Dict: 字典对象，如 {"name": "{{username}}"}
        - str: JSON字符串，支持非字符串类型的模板变量，如 '{"id": {{userId}}}'
               其中 {{userId}} 会被替换为整数而不是字符串
    :param form_data: 请求form表单
    :multipart_options: 附件
    :param headers: 请求头
    :param cookies: cookie
    :param assert_options: 断言参数
    :param think_time_option: 思考时间
    :param setup_options: 接口初始化选项
    :param teardown_options: 接口后置选项，每次请求完成后执行，
        使用setup_option()函数创建，支持 {{key}} 模板语法。
    """

def setup_option(
    name: str,
    url: str,
    method: str,
    json: Dict | None = None,
    form_data: Dict | None = None,
    multipart_options: List[Dict] | None = None,
    headers: Dict | None = None,
    cookies: str | None = None,
    jsonpath_extract: List | None = None,
) -> Dict[str, Any]:
    """
    初始化选项
    :param name: 接口名称
    :param url: 接口地址
    :param method: 请求方法
    :param json: 请求json
    :param form_data: 请求form表单
    :multipart_options: 附件
    :param headers: 请求头
    :param cookies: cookie
    :param jsonpath_extract: 通过jsonpath提取参数
    :return:
    """

def jsonpath_extract_option(key: str, jsonpath: str) -> Dict[str, str]:
    """
    jsonpath提取参数设置
    :param key: 全局key
    :param jsonpath: 提取jsonpath路径
    :return:
    """

def multipart_option(form_key: str, path: str, file_name: str, mime: str) -> Dict:
    """
    上传附件选项
    :param form_key: form表单的key，根据服务端选择，e.g: file， file1
    :param path: 文件路径
    :param file_name: 文件名
    :param mime: 文件类型，e.g: application/octet-stream,可以参考:https://developer.mozilla.org/zh-CN/docs/Web/HTTP/Basics_of_HTTP/MIME_types/Common_types
    """

def data_pool_option(file_path: str, mode: str = "sequential") -> Dict[str, Any]:
    """
    数据池配置，用于参数化测试
    CSV文件格式：第一行为字段名（逗号分隔），后续行为数据值
    在请求参数中使用 {{字段名}} 来引用数据池中的值

    :param file_path: CSV文件路径
    :param mode: 读取模式
        - "sequential": 循环顺序读取（默认），数据用完后从头开始
        - "random": 随机读取
    :return: 数据池配置字典

    示例CSV文件:
        username,password,age
        user1,pass1,20
        user2,pass2,25

    使用方式:
        endpoint(
            name="登录",
            url="http://api.example.com/login",
            method="POST",
            weight=1,
            json={"username": "{{username}}", "password": "{{password}}"}
        )
    """

class BatchRunner:
    def __init__(self) -> None: ...
    async def run(
        self,
        test_duration_secs: int,
        concurrent_requests: int,
        api_endpoints: List[Dict],
        step_option: Dict[str, int] | None = None,
        setup_options: List[Dict[str, Any]] | None = None,
        verbose: bool = False,
        should_prevent: bool = False,
        assert_channel_buffer_size: int = 1024,
        timeout_secs=0,
        cookie_store_enable=True,
        ema_alpha: float = 0,
        data_pool: Dict[str, Any] | None = None,
        global_variables: Dict[str, Any] | None = None,
        teardown_options: List[Dict[str, Any]] | None = None,
    ) -> None:
        """
        批量压测
        :param test_duration_secs: 测试持续时间
        :param concurrent_requests: 并发数
        :param api_endpoints: 接口信息
        :param step_option: 阶梯加压选项
        :param setup_options: 初始化选项
        :param verbose: 打印详细信息
        :param should_prevent: 是否禁用睡眠
        :param assert_channel_buffer_size: 断言队列buffer大小
        :param timeout_secs: http超时时间
        :param cookie_store_enable: 是否为客户端启用持久性cookie存储。
        :param ema_alpha: 指数滑动平均参数，0-1之间,0为不使用，值越大曲线越平滑，但是越失真，建议使用0.1以下
        :param data_pool: 数据池配置，使用data_pool_option()函数创建
        :param global_variables: 全局变量字典，在所有接口中可通过 {{key}} 模板语法引用。
            优先级：全局变量 < 全局setup提取 < CSV数据池 < 接口级setup提取
        :param teardown_options: 全局teardown选项，压测结束后执行（无论正常结束还是手动stop），
            使用setup_option()函数创建，支持 {{key}} 模板语法引用全局变量和setup提取的值。
            teardown执行失败不影响结果输出，仅打印错误日志。
        """
        ...

    async def stop(self) -> None:
        """
        停止压测（异步方法）
        设置停止标志，中止后台任务，清理资源。
        """
        ...

    def __aiter__(self) -> "BatchRunner": ...
    async def __anext__(self) -> Any:
        """
        异步迭代获取下一个压测结果。
        用法: async for msg in runner: ...
        """
        ...

    def __iter__(self) -> "BatchRunner": ...
    def __next__(self) -> Optional[Any]: ...


# ============================================================
# WebSocket 压测 API (独立于 HTTP, 与现有功能完全隔离)
# ============================================================

def ws_text(value: str) -> Dict[str, Any]:
    """构造 WebSocket 文本消息模板, value 支持 {{key}} 模板语法"""

def ws_binary(value: bytes) -> Dict[str, Any]:
    """构造 WebSocket 二进制消息模板"""

def ws_json(value: Dict | List | Any) -> Dict[str, Any]:
    """构造 WebSocket JSON 消息模板, 字段值支持 {{key}} 模板语法"""

def ws_match_sequential() -> Dict[str, Any]:
    """FIFO 顺序匹配策略 (RequestResponse 模式)"""

def ws_match_jsonpath(send_path: str, recv_path: str) -> Dict[str, Any]:
    """JSONPath 字段匹配策略, 发送时按 send_path 取值, 接收时按 recv_path 取值"""

def ws_mode_oneway() -> Dict[str, Any]:
    """OneWay 模式: 不做请求-响应配对, 仅统计 send / recv 数"""

def ws_mode_request_response(
    match_by: Dict[str, Any], timeout_ms: int = 30_000
) -> Dict[str, Any]:
    """RequestResponse 模式: 按 match_by 配对, 统计 send→recv RTT"""

def ws_send_pattern(
    messages: List[Dict[str, Any]],
    rate_per_sec: float | None = None,
    interval_ms: int | None = None,
    iterate_data_pool: bool = False,
) -> Dict[str, Any]:
    """周期性发送配置. rate_per_sec 与 interval_ms 二选一."""

def ws_heartbeat(
    interval_secs: int,
    timeout_secs: int,
    payload: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """心跳配置, payload 为 None 时使用 ws ping frame"""

def ws_reconnect_policy(max_attempts: int, backoff_ms: int) -> Dict[str, Any]:
    """重连策略"""

def ws_assert_option(
    jsonpath: str,
    reference_object: Any,
    on_match: str = "EveryMessage",
) -> Dict[str, Any]:
    """WS 断言. on_match: EveryMessage | FirstMatch | NoMatch"""

def ws_endpoint(
    name: str,
    url: str,
    weight: int,
    mode: Dict[str, Any],
    subprotocols: List[str] | None = None,
    headers: Dict | None = None,
    on_connect: List[Dict] | None = None,
    send_pattern: Dict | None = None,
    heartbeat: Dict | None = None,
    connection_ttl_secs: int | None = None,
    reconnect: Dict | None = None,
    assert_options: List[Dict] | None = None,
    think_time_option: Dict | None = None,
    setup_options: List[Dict] | None = None,
    teardown_options: List[Dict] | None = None,
) -> Dict[str, Any]:
    """
    构造 WebSocket 接入点配置
    :param url: ws:// 或 wss://, 支持 {{key}} 模板
    :param weight: 权重, 决定该 endpoint 分到的连接数
    :param mode: 使用 ws_mode_oneway() 或 ws_mode_request_response()
    :param on_connect: 连接建立后立即顺序发送的初始消息
    :param send_pattern: 周期性发送配置
    :param heartbeat: 心跳配置
    :param connection_ttl_secs: 单连接最大持续时间, None 时跟随 batch 总时长
    :param reconnect: 重连策略
    :param setup_options: HTTP setup, 在 WS 握手前发起 (鉴权场景)
    """

class WsBatchRunner:
    """
    WebSocket 压测运行器, 与 BatchRunner 同构但完全独立.
    """

    def __init__(self) -> None: ...
    async def run(
        self,
        test_duration_secs: int,
        concurrent_connections: int,
        ws_endpoints: List[Dict],
        step_option: Dict[str, int] | None = None,
        setup_options: List[Dict[str, Any]] | None = None,
        verbose: bool = False,
        should_prevent: bool = False,
        assert_channel_buffer_size: int = 1024,
        timeout_secs: int = 0,
        cookie_store_enable: bool = True,
        data_pool: Dict[str, Any] | None = None,
        global_variables: Dict[str, Any] | None = None,
        teardown_options: List[Dict[str, Any]] | None = None,
    ) -> None:
        """
        WebSocket 批量压测
        :param test_duration_secs: 测试持续时间
        :param concurrent_connections: 总连接数 (按 weight 分配到各 endpoint)
        :param ws_endpoints: 使用 ws_endpoint() 构造的列表
        :param step_option: 阶梯加压, 与 HTTP 的 BatchRunner 同义
        :param setup_options: 全局 HTTP setup, 在压测开始前执行 (鉴权)
        :param data_pool: CSV 数据池, 与 HTTP 共用
        :param global_variables: 全局变量字典, 模板渲染时可引用
        """
        ...

    async def stop(self) -> None:
        """停止 WS 压测, 关闭所有连接"""
        ...

    def __aiter__(self) -> "WsBatchRunner": ...
    async def __anext__(self) -> Any: ...
    def __iter__(self) -> "WsBatchRunner": ...
    def __next__(self) -> Optional[Any]: ...
