#!/usr/bin/env bash
set -euo pipefail
# 创建output目录
echo "创建output目录..."
mkdir -p output
echo "开始构建 lance-ray..."
# 构建库与CLI（不含auth特性）
uv build
# 拷贝二进制产物到output目录
echo "拷贝二进制产物到output目录..."
cp dist/lance_ray-* output/
echo "构建完成！二进制产物已拷贝到output目录："
ls -la output/