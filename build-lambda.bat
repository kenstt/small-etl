@echo off
REM AWS Lambda 構建腳本 (Windows版本)
REM 使用 cargo lambda 工具

echo Building for AWS Lambda using cargo lambda...

REM 檢查是否已安裝 cargo lambda
cargo lambda --version >nul 2>&1
if errorlevel 1 (
    echo Installing cargo lambda...
    cargo install cargo-lambda
)

REM 使用 cargo lambda 構建
cargo lambda build --bin lambda --release

if errorlevel 1 (
    echo Build failed!
    exit /b 1
)

echo Build completed. Binary location:
echo target\lambda\lambda\bootstrap

echo.
echo Creating deployment package...

REM 創建部署包
if exist "target\lambda\lambda\bootstrap" (
    pushd target\lambda\lambda
    powershell Compress-Archive -Path bootstrap -DestinationPath ..\..\..\lambda-deployment.zip -Force
    popd
    echo ✅ Deployment package created: lambda-deployment.zip
    echo.
    echo Ready to deploy to AWS Lambda!
) else (
    echo ❌ Binary not found. Build may have failed.
)