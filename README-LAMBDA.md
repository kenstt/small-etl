# AWS Lambda 部署指南

## 系統需求

1. 安裝Linux target:
```bash
rustup target add x86_64-unknown-linux-musl
```

## 構建Lambda函數

### Windows用戶
```cmd
build-lambda.bat
```

### Linux/Mac用戶
```bash
chmod +x build-lambda.sh
./build-lambda.sh
```

## 手動構建
```bash
cargo build --bin lambda --features lambda --target x86_64-unknown-linux-musl --release
```

## 部署到AWS Lambda

### 1. 創建部署包
```bash
# 複製二進制文件為 bootstrap (Lambda custom runtime要求)
cp target/x86_64-unknown-linux-musl/release/lambda bootstrap

# 創建ZIP包
zip lambda-deployment.zip bootstrap
```

### 2. AWS CLI部署
```bash
# 創建Lambda函數
aws lambda create-function \
  --function-name samll-etl \
  --runtime provided.al2 \
  --role arn:aws:iam::ACCOUNT:role/lambda-execution-role \
  --handler bootstrap \
  --zip-file fileb://lambda-deployment.zip \
  --timeout 300 \
  --memory-size 512

# 更新函數代碼
aws lambda update-function-code \
  --function-name samll-etl \
  --zip-file fileb://lambda-deployment.zip
```

### 3. 環境變量設置
```bash
aws lambda update-function-configuration \
  --function-name samll-etl \
  --environment Variables='{
    "S3_BUCKET":"your-bucket-name",
    "API_ENDPOINT":"https://jsonplaceholder.typicode.com/posts",
    "S3_PREFIX":"etl-output",
    "CONCURRENT_REQUESTS":"5"
  }'
```

### 4. IAM權限
Lambda執行角色需要以下權限:
- `AWSLambdaBasicExecutionRole` (日誌權限)
- S3讀寫權限:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": "arn:aws:s3:::your-bucket-name/*"
    }
  ]
}
```

## 測試Lambda函數

### 測試事件範例
```json
{
  "api_endpoint": "https://jsonplaceholder.typicode.com/posts",
  "s3_bucket": "your-bucket-name",
  "s3_prefix": "etl-output"
}
```

### 使用AWS CLI測試
```bash
aws lambda invoke \
  --function-name samll-etl \
  --payload '{"api_endpoint":"https://jsonplaceholder.typicode.com/posts"}' \
  response.json

cat response.json
```

## 本地測試
由於使用了Linux target，無法在Windows上直接運行Lambda二進制文件。建議:
1. 使用CLI版本進行本地測試: `cargo run --features cli`
2. 使用Docker進行Lambda環境模擬
3. 部署到AWS後進行測試