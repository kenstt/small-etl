# 使用範例

## 1. 快速開始 - MVP 模式

### 步驟 1: 查看可用的配置範例
```bash
ls configs/
# mvp-simple.toml      - MVP 簡化配置
# dev-testing.toml     - 開發測試配置
# production-example.toml - 生產環境範例
```

### 步驟 2: 使用 MVP 配置進行 Dry Run
```bash
cargo run --bin toml_etl -- --config configs/mvp-simple.toml --dry-run
```

預期輸出：
```
📋 Configuration Summary:
  Pipeline: mvp-simple v1.0.0
  Source: https://jsonplaceholder.typicode.com/posts
  Output: ./output
  MVP Mode: true
  Formats: csv, json
  Max Records: 1
  Concurrent Requests: 1

🔍 Dry Run Analysis:

📡 Data Source Analysis:
  Endpoint: https://jsonplaceholder.typicode.com/posts
  Method: GET

⚙️ Processing Mode:
  🎯 MVP Mode: Will process ONLY the first record
  📊 Expected records: 1

💾 Output Configuration:
  Path: ./output
  Formats: csv, json
  Compression: mvp_output.zip (ZIP)

✅ Dry run analysis complete.
```

### 步驟 3: 執行 MVP 處理
```bash
cargo run --bin toml_etl -- --config configs/mvp-simple.toml --verbose
```

預期輸出：
```
🚀 Starting TOML-based ETL tool
📁 Loading configuration from: configs/mvp-simple.toml
✅ Configuration loaded and validated successfully
🚀 Starting MVP extraction from: https://jsonplaceholder.typicode.com/posts
📋 MVP Mode enabled - will process only first record
✅ MVP Mode: Successfully extracted first record
📊 Extracted 1 records
🔄 Starting MVP transformation for 1 records
✅ MVP Mode: Processed first record successfully
📋 Transformation complete: 1 processed, 0 intermediate
💾 Starting MVP load to: ./output/mvp_output.zip
✅ MVP load completed successfully
✅ ETL process completed successfully!
📁 Output saved to: ./output/mvp_output.zip
```

## 2. 進階配置範例

### 使用環境變數
```bash
# 設定環境變數
export API_ENDPOINT="https://my-api.com/data"
export OUTPUT_PATH="./custom-output"

# 使用包含環境變數的配置
cargo run --bin toml_etl -- --config configs/production-example.toml
```

### 覆蓋配置選項
```bash
# 強制啟用 MVP 模式
cargo run --bin toml_etl -- --mvp true

# 強制啟用監控
cargo run --bin toml_etl -- --monitor --verbose

# 組合使用
cargo run --bin toml_etl -- --config configs/dev-testing.toml --mvp false --verbose
```

## 3. 自訂配置範例

### 創建自己的配置文件
```toml
# my-config.toml
[pipeline]
name = "my-custom-etl"
description = "自訂 ETL 流程"
version = "1.0.0"

[source]
type = "api"
endpoint = "https://api.github.com/users"
timeout_seconds = 15

[source.headers]
"User-Agent" = "MyApp/1.0"

[extract]
first_record_only = false
max_records = 5
concurrent_requests = 2

[extract.field_mapping]
login = "username"
id = "user_id"
html_url = "profile_url"

[transform]

[transform.operations]
trim_whitespace = true
clean_text = true

[transform.validation]
required_fields = ["username", "user_id"]

[load]
output_path = "./github-users"
output_formats = ["csv", "json"]

[load.compression]
enabled = true
filename = "github_users.zip"
```

### 使用自訂配置
```bash
cargo run --bin toml_etl -- --config my-config.toml --dry-run
```

## 4. 錯誤處理範例

### API 失敗時使用範例數據
```toml
[error_handling]
on_api_failure = "use_sample_data"
on_transform_error = "skip_record"
on_load_error = "fail"
```

### 重試機制
```toml
[source]
retry_attempts = 3
retry_delay_seconds = 5
timeout_seconds = 30
```

## 5. 開發和調試

### 啟用詳細日誌
```bash
cargo run --bin toml_etl -- --verbose --config configs/dev-testing.toml
```

### 使用測試端點
```toml
[source]
endpoint = "https://httpbin.org/json"  # 返回固定 JSON 的測試端點
```

### 監控系統資源
```bash
cargo run --bin toml_etl -- --monitor --config configs/mvp-simple.toml
```

## 6. 生產部署

### 準備生產配置
```toml
[pipeline]
name = "production-etl"
version = "2.0.0"

[source]
endpoint = "${PRODUCTION_API_ENDPOINT}"
timeout_seconds = 60
retry_attempts = 5

[source.headers]
"Authorization" = "Bearer ${API_TOKEN}"

[extract]
first_record_only = false
concurrent_requests = 20
max_records = 10000

[performance]
memory_limit_mb = 2048
request_timeout = 60

[monitoring]
enabled = true
log_level = "info"
system_stats = true
```

### 設定環境變數
```bash
export PRODUCTION_API_ENDPOINT="https://prod-api.company.com/data"
export API_TOKEN="your-production-token"
export LOG_LEVEL="info"
```

### 執行生產作業
```bash
cargo run --bin toml_etl -- --config production.toml --monitor
```

## 7. 輸出檔案結構

執行成功後，會產生 ZIP 檔案包含：

```
output/
└── etl_output.zip
    ├── output.csv          # CSV 格式數據
    ├── output.tsv          # TSV 格式數據
    ├── processed_data.json # 完整處理數據
    └── intermediate.json   # 中繼數據（如果有）
```

### CSV 輸出範例
```csv
id,title,body,userId,processed
1,"Sample Post 1","This is sample content...",1,true
```

### JSON 輸出範例
```json
[
  {
    "data": {
      "id": 1,
      "title": "Sample Post 1",
      "body": "This is sample content...",
      "userId": 1,
      "processed": true
    }
  }
]
```

## 8. 常見問題排解

### 配置文件錯誤
```bash
❌ Failed to load config file 'bad-config.toml': TOML parsing error: ...
💡 Make sure the file exists and is valid TOML format
```
**解決方法**: 檢查 TOML 語法，確保所有字串都用引號包圍

### API 連接失敗
```bash
❌ ETL process failed: API request failed
💡 建議: Check network connectivity and API service status
```
**解決方法**: 檢查網路連線，或啟用錯誤處理使用範例數據

### 權限錯誤
```bash
❌ ETL process failed: Permission denied
💡 建議: Check file permissions and directory access
```
**解決方法**: 確保輸出目錄可寫入，或更改輸出路徑