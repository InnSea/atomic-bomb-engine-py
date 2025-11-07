#!/bin/bash
# 一键编译、测试并推送到 GitHub

set -e

ROOT_DIR=$(pwd)

echo "🚀 开始构建并推送..."
echo ""

# 1. 测试编译
echo "📦 步骤 1/2: 测试编译..."
cd "$ROOT_DIR/atomic-bomb-engine-py"

if maturin build --release > /tmp/maturin_build.log 2>&1; then
    echo "✅ 编译成功"
else
    echo "❌ 编译失败！"
    echo ""
    tail -20 /tmp/maturin_build.log
    echo ""
    echo "完整日志: /tmp/maturin_build.log"
    exit 1
fi

echo ""

# 2. 提交并推送
echo "📦 步骤 2/2: 提交并推送..."
cd "$ROOT_DIR/atomic-bomb-engine-py"

if git diff --quiet && git diff --cached --quiet; then
    echo "ℹ️  没有需要提交的修改"
else
    git add .
    
    # 提示用户输入commit信息
    echo "请输入commit信息 (默认: feat: 更新代码):"
    read -r commit_msg
    if [ -z "$commit_msg" ]; then
        commit_msg="feat: 更新代码"
    fi
    
    git commit -m "$commit_msg" || true
    
    echo "🚀 推送到 GitHub..."
    current_branch=$(git branch --show-current)
    git push origin "$current_branch"
    
    echo ""
    echo "✅ 推送成功！"
    echo ""
    echo "🔗 查看构建: https://github.com/InnSea/atomic-bomb-engine-py/actions"
fi

cd "$ROOT_DIR"

echo ""
echo "💡 提示: 以后直接修改 atomic-bomb-engine-py/atomic-bomb-engine/ 目录下的代码即可"
