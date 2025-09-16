# API 鏈式呼叫範例

這個範例展示如何使用 samll-etl 從第一個 API 取得特定值，然後作為參數呼叫下一個 API。

## 功能概述

- **第一個 Pipeline**: 從 API 取得用戶基本資訊
- **第二個 Pipeline**: 使用第一個 Pipeline 取得的 `user_id` 作為參數，呼叫用戶詳細資訊 API

## 配置範例

檔案：`configs/simple-demo.toml`

```toml
[sequence]
name = "simple-demo"
description = "演示：取得第一個用戶 ID -> 取得該用戶詳情"
version = "1.0.0"
execution_order = ["get-first-user", "get-user-detail"]

# Pipeline 1: 取得第一個用戶
[[pipelines]]
name = "get-first-user"
description = "取得第一個用戶的基本資訊"
enabled = true

[pipelines.source]
type = "api"
endpoint = "https://jsonplaceholder.typicode.com/users/1"

[pipelines.extract.field_mapping]
id = "user_id"          # 將 'id' 字段映射為 'user_id'
name = "name"
email = "email"

# Pipeline 2: 使用用戶 ID 取得詳細資訊
[[pipelines]]
name = "get-user-detail"
description = "使用用戶 ID 取得完整用戶資訊"
enabled = true
dependencies = ["get-first-user"]

[pipelines.source]
type = "api"
endpoint = "https://jsonplaceholder.typicode.com/users/{user_id}"  # 使用參數化端點

[pipelines.source.data_source]
use_previous_output = true
from_pipeline = "get-first-user"

[pipelines.extract.field_mapping]
id = "detail_id"
name = "full_name"
email = "email_address"
phone = "phone_number"
website = "website"
```

## 關鍵特性

### 1. 參數化 API 端點
```toml
endpoint = "https://jsonplaceholder.typicode.com/users/{user_id}"
```
使用 `{參數名}` 語法在 URL 中定義參數佔位符。

### 2. 字段映射
```toml
[pipelines.extract.field_mapping]
id = "user_id"  # API 回應中的 'id' -> 內部使用的 'user_id'
```

### 3. Pipeline 數據傳遞
```toml
[pipelines.source.data_source]
use_previous_output = true      # 使用前一個 Pipeline 的輸出
from_pipeline = "get-first-user"  # 指定來源 Pipeline
```

## 執行方式

### 乾運行（檢查配置）
```bash
cargo run --bin sequence_etl -- --config configs/simple-demo.toml --dry-run
```

### 實際執行
```bash
cargo run --bin sequence_etl -- --config configs/simple-demo.toml
```

## 執行流程

1. **第一個 Pipeline** (`get-first-user`):
   - 呼叫 `https://jsonplaceholder.typicode.com/users/1`
   - 應用字段映射：`id` → `user_id`
   - 儲存結果

2. **第二個 Pipeline** (`get-user-detail`):
   - 從第一個 Pipeline 獲取數據
   - 將 `{user_id}` 替換為實際的用戶 ID (例如：1)
   - 呼叫 `https://jsonplaceholder.typicode.com/users/1`
   - 應用字段映射並儲存結果

## 執行結果範例

```
📡 get-user-detail: Replaced {user_id} with 1

📊 Execution Results Summary:
  Execution ID: seq_20250916_041558
  Completed Pipelines: 2
  Total Records Processed: 2
  Total Execution Time: 123.9822ms

📝 Pipeline Details:
  1. get-first-user - 1 records in 74.4886ms
  2. get-user-detail - 1 records in 49.4936ms
```

## 進階用法

### 批量處理
如果第一個 Pipeline 返回多個記錄，第二個 Pipeline 會為每個記錄分別呼叫 API：

```toml
# 第一個 Pipeline 取得多個用戶
[[pipelines]]
name = "get-users"
[pipelines.source]
endpoint = "https://jsonplaceholder.typicode.com/users"
[pipelines.extract]
max_records = 3  # 只取前 3 個用戶

# 第二個 Pipeline 會為每個用戶 ID 呼叫詳細資訊 API
[[pipelines]]
name = "get-user-details"
[pipelines.source]
endpoint = "https://jsonplaceholder.typicode.com/users/{user_id}"
```

這樣會產生 3 次 API 呼叫：
- `https://jsonplaceholder.typicode.com/users/1`
- `https://jsonplaceholder.typicode.com/users/2`
- `https://jsonplaceholder.typicode.com/users/3`

### 錯誤處理
如果參數無法解析（例如缺少必要字段），系統會提供詳細的錯誤訊息：

```
❌ Unresolved parameters in endpoint: https://api.example.com/users/{missing_field}
Available fields: ["user_id", "name", "email"]
```

## 測試

系統包含完整的測試覆蓋：
- API 鏈正常執行測試
- 缺少參數錯誤處理測試
- 字段映射測試

```bash
cargo test --test simple_api_chain_test
```