@echo off
REM AWS Lambda 構建腳本 (Windows版本)

echo Building for AWS Lambda (x86_64-unknown-linux-musl)...

REM 安裝target (如果尚未安裝)
rustup target add x86_64-unknown-linux-musl

REM 使用musl target構建 (適合Lambda)
cargo build --bin lambda --features lambda --target x86_64-unknown-linux-musl --release

echo Build completed. Binary location:
echo target\x86_64-unknown-linux-musl\release\lambda

echo.
echo To deploy to Lambda:
echo 1. Copy the binary to 'bootstrap' (Lambda custom runtime requirement)
echo 2. Zip the bootstrap file
echo 3. Upload to Lambda

REM 創建部署包
if exist "target\x86_64-unknown-linux-musl\release\lambda.exe" (
    echo.
    echo Creating deployment package...
    copy "target\x86_64-unknown-linux-musl\release\lambda.exe" bootstrap
    powershell Compress-Archive -Path bootstrap -DestinationPath lambda-deployment.zip -Force
    del bootstrap
    echo ✅ Deployment package created: lambda-deployment.zip
) else if exist "target\x86_64-unknown-linux-musl\release\lambda" (
    echo.
    echo Creating deployment package...
    copy "target\x86_64-unknown-linux-musl\release\lambda" bootstrap
    powershell Compress-Archive -Path bootstrap -DestinationPath lambda-deployment.zip -Force
    del bootstrap
    echo ✅ Deployment package created: lambda-deployment.zip
)