#!/bin/bash
# 
# 用于构建 Linux 版本的 atomic-bomb-engine wheel 包
# 适用于 Docker 部署场景
#

set -e

# 颜色输出
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${BLUE}=====================================${NC}"
echo -e "${BLUE}构建 atomic-bomb-engine Linux 版本${NC}"
echo -e "${BLUE}=====================================${NC}\n"

# 检查 Docker 是否运行
if ! docker info > /dev/null 2>&1; then
    echo -e "${YELLOW}错误: Docker 未运行，请先启动 Docker${NC}"
    exit 1
fi

# 获取脚本所在目录
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

echo -e "${GREEN}1. 清理之前的构建产物...${NC}"
rm -rf target/wheels/*.whl 2>/dev/null || true

echo -e "${GREEN}2. 使用 Docker 构建 Linux 版本的 wheel 包...${NC}"
echo -e "${YELLOW}   这可能需要几分钟时间...${NC}\n"

# 构建多个 Python 版本
docker run --rm -v "$(pwd)":/io \
  ghcr.io/pyo3/maturin build --release \
  -i python3.12 \
  -i python3.11 \
  -i python3.10 \
  -i python3.9 \
  -i python3.8

# 检查构建是否成功
if [ $? -eq 0 ]; then
    echo -e "\n${GREEN}✓ 构建成功！${NC}\n"
    echo -e "${BLUE}生成的 wheel 包：${NC}"
    ls -lh target/wheels/*.whl
    
    echo -e "\n${BLUE}=====================================${NC}"
    echo -e "${GREEN}下一步操作：${NC}"
    echo -e "${BLUE}=====================================${NC}"
    echo -e "1. 在你的项目中创建 wheels 目录："
    echo -e "   ${YELLOW}mkdir -p /path/to/your/project/wheels${NC}\n"
    echo -e "2. 复制 wheel 包到你的项目："
    echo -e "   ${YELLOW}cp target/wheels/*.whl /path/to/your/project/wheels/${NC}\n"
    echo -e "3. 在 Dockerfile 中添加："
    echo -e "   ${YELLOW}COPY wheels/*.whl /tmp/wheels/${NC}"
    echo -e "   ${YELLOW}RUN pip install --no-cache-dir /tmp/wheels/*.whl${NC}\n"
    echo -e "4. 构建 Docker 镜像："
    echo -e "   ${YELLOW}cd /path/to/your/project && docker build -t your-image .${NC}\n"
else
    echo -e "\n${YELLOW}✗ 构建失败${NC}"
    exit 1
fi

