# TOML ETL 配置指南

## 快速開始

### 使用 MVP 模式 (推薦)

```bash
# 使用預設配置進行 dry run 分析
cargo run --bin toml_etl -- --dry-run

# 使用 MVP 簡化配置
cargo run --bin toml_etl -- -c configs/mvp-simple.toml

# 強制啟用 MVP 模式
cargo run --bin toml_etl -- --mvp true

# 啟用詳細輸出和監控
cargo run --bin toml_etl -- --verbose --monitor
```

## 配置文件結構

### 核心區塊

1. **[pipeline]** - 管道基本信息
2. **[source]** - 數據源配置
3. **[extract]** - 擷取設定
4. **[transform]** - 轉換規則
5. **[load]** - 輸出配置

### MVP 關鍵設定

```toml
[extract]
first_record_only = true  # 啟用 MVP 模式
max_records = 1          # 最大記錄數
concurrent_requests = 1   # 降低並發數
```

## 配置範例

### 1. MVP 簡化配置 (`configs/mvp-simple.toml`)
- ✅ 只處理第一筆記錄
- ✅ 最小化資源使用
- ✅ 快速驗證概念

### 2. 開發測試配置 (`configs/dev-testing.toml`)
- ✅ 使用測試API端點
- ✅ 少量數據處理
- ✅ 詳細日誌輸出

### 3. 生產環境配置 (`configs/production-example.toml`)
- ✅ 完整功能配置
- ✅ 錯誤處理和重試
- ✅ 性能優化設定

## 字段映射

重新映射 API 響應字段：

```toml
[extract.field_mapping]
id = "post_id"
title = "post_title"
body = "post_content"
userId = "author_id"
```

## 轉換操作

```toml
[transform.operations]
clean_text = true           # 清理文本
trim_whitespace = true      # 去除空白
remove_html_tags = false    # 移除HTML標籤

[transform.validation]
required_fields = ["post_id", "post_title"]
max_title_length = 200

[transform.intermediate]
title_length_threshold = 50  # 標題長度 > 50 的記錄進入中繼數據
```

## 輸出格式

```toml
[load]
output_path = "./output"
output_formats = ["csv", "tsv", "json"]  # 支援多種格式

[load.compression]
enabled = true
filename = "etl_output.zip"
include_intermediate = true
```

## 環境變數

使用 `${VAR_NAME}` 語法：

```toml
[source]
endpoint = "${API_ENDPOINT}"

[source.headers]
"Authorization" = "Bearer ${API_TOKEN}"
```

## 命令列選項

```bash
# 基本選項
--config, -c        配置文件路徑 (預設: etl-config.toml)
--verbose, -v       詳細輸出
--dry-run          預覽模式，不執行實際處理
--mvp              強制啟用/停用 MVP 模式
--monitor          啟用系統監控
```

## Dry Run 分析

在實際執行前分析配置：

```bash
cargo run --bin toml_etl -- --dry-run -c configs/mvp-simple.toml
```

輸出範例：
```
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
```

## 錯誤處理

```toml
[error_handling]
on_api_failure = "use_sample_data"    # "fail", "use_sample_data", "retry"
on_transform_error = "skip_record"    # "fail", "skip_record", "use_default"
on_load_error = "fail"                # "fail", "retry"
```

## 性能調優

```toml
[performance]
request_timeout = 30
memory_limit_mb = 512
disk_cache_enabled = false

[extract]
concurrent_requests = 1  # MVP: 降低並發
```

## 監控設定

```toml
[monitoring]
enabled = true
log_level = "info"        # debug, info, warn, error
system_stats = true      # CPU/Memory 使用率
performance_metrics = true
```

## 最佳實踐

### MVP 開發階段
1. 使用 `first_record_only = true`
2. 設定 `max_records = 1`
3. 降低 `concurrent_requests = 1`
4. 啟用詳細日誌輸出

### 生產部署
1. 關閉 MVP 模式
2. 調整並發數和超時設定
3. 配置錯誤處理策略
4. 設定環境變數保護敏感信息

### 開發調試
1. 使用 `--dry-run` 預覽配置
2. 設定 `log_level = "debug"`
3. 使用測試 API 端點
4. 啟用監控查看性能指標