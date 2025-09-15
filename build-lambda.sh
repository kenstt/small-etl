#!/bin/bash

# AWS Lambda 構建腳本
# 使用 cargo lambda 工具

echo "Building for AWS Lambda using cargo lambda..."

# 檢查是否已安裝 cargo lambda
if ! command -v cargo-lambda &> /dev/null; then
    echo "Installing cargo lambda..."
    cargo install cargo-lambda
fi

# 使用 cargo lambda 構建
cargo lambda build --bin lambda --release

echo "Build completed. Binary location:"
echo "target/lambda/lambda/bootstrap"

echo ""
echo "Creating deployment package..."

# 創建部署包
if [ -f "target/lambda/lambda/bootstrap" ]; then
    cd target/lambda/lambda
    zip ../../../lambda-deployment.zip bootstrap
    cd ../../..
    echo "✅ Deployment package created: lambda-deployment.zip"
    echo ""
    echo "Ready to deploy to AWS Lambda!"
else
    echo "❌ Binary not found. Build may have failed."
fi