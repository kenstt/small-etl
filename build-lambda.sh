#!/bin/bash

# AWS Lambda 構建腳本
# 需要安裝 cross 工具: cargo install cross

echo "Building for AWS Lambda (x86_64-unknown-linux-musl)..."

# 安裝target (如果尚未安裝)
rustup target add x86_64-unknown-linux-musl

# 使用musl target構建 (適合Lambda)
cargo build --bin lambda --features lambda --target x86_64-unknown-linux-musl --release

echo "Build completed. Binary location:"
echo "target/x86_64-unknown-linux-musl/release/lambda"

echo ""
echo "To deploy to Lambda:"
echo "1. Copy the binary to 'bootstrap' (Lambda custom runtime requirement)"
echo "2. Zip the bootstrap file"
echo "3. Upload to Lambda"

# 創建部署包
if [ -f "target/x86_64-unknown-linux-musl/release/lambda" ]; then
    echo ""
    echo "Creating deployment package..."
    cp target/x86_64-unknown-linux-musl/release/lambda bootstrap
    zip lambda-deployment.zip bootstrap
    rm bootstrap
    echo "✅ Deployment package created: lambda-deployment.zip"
fi