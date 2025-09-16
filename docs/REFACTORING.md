# 專案資料夾重構建議（第一階段：資料夾架構）

本文檔針對當前專案的實際內容，提供「僅調整資料夾架構」的重構建議，目的在於：
- 更清楚地劃分 Domain（核心模型/協議）、Adapters（外部依賴）、Application（用例/流程）層次
- 將 CLI 與 Lambda 入口與功能隔離，降低 feature gate 交叉干擾
- 讓各 Pipeline 的職責與共用元件清晰、可擴充
- 在不破壞現有功能的前提下，規劃可分步實施的遷移路徑

注意：本階段僅提供結構與對應關係，未實際移動檔案；待確認後可逐步遷移。

---

## 一、目前主要結構（概覽）

- src/
  - main.rs（CLI 入口）
  - lambda.rs（Lambda 入口，受 feature `lambda` 控制）
  - lib.rs（公開模組匯出）
  - config/
    - cli.rs、lambda.rs、toml_config.rs、sequence_config.rs、mod.rs
  - core/
    - mod.rs（定義 Record、Storage、ConfigProvider、Pipeline 等 trait）
    - etl.rs（EtlEngine）
    - pipeline.rs、mvp_pipeline.rs、pipeline_sequence.rs、contextual_pipeline.rs
  - utils/
    - error.rs、logger.rs、monitor.rs、validation.rs、mod.rs
  - bin/
    - sequence_etl.rs、toml_etl.rs（額外範例或專用執行檔）

- 根目錄
  - Cargo.toml（features: default=cli, lambda 可選；[[bin]] 指向 src/lambda.rs）
  - configs/（多個設定範例，如 sequence-example.toml）
  - output/、simple-output/、demo-output/（輸出樣例）
  - tests/（整合測試）

問題點與風險：
- core 同時承載 Domain 模型和流程引擎，易與 adapters/infra 職責混雜。
- config 與 storage、http 等外部交互概念尚未分層，擴充新存儲（S3、本地、DB）或新來源（HTTP、File）時耦合度偏高。
- lambda.rs 作為單一檔案 bin，後續擴充 Lambda handler / infra 抽象不易。
- bin/ 下的範例可與 examples/ 或 apps/ 劃分更清楚。

---

## 二、建議的目標資料夾架構（分層）

採用「應用層 / 域層 / 適配層」的分層，對齊未來可擴展性：

- src/
  - app/（應用層：組裝用例、流程與入口）
    - cli/
      - main.rs（CLI 入口；現有 src/main.rs 遷入）
      - commands/（如後續擴充多子命令，可拆檔）
    - lambda/
      - main.rs（Lambda 入口；現有 src/lambda.rs 遷入，並保留 feature gate）
    - pipelines/（應用場景的組裝，依賴 domain 與 adapters 的抽象）
      - simple_pipeline.rs（對應現有 core/pipeline.rs 的「用例編排」部分）
      - mvp_pipeline.rs（對應現有 core/mvp_pipeline.rs 的「用例編排」部分）
      - sequence_pipeline.rs（對應 core/pipeline_sequence.rs）
      - contextual_pipeline.rs（對應 core/contextual_pipeline.rs）
  - domain/（領域層：不依賴具體基礎設施，僅定義模型與協議）
    - mod.rs（集中 re-export）
    - model.rs（Record、TransformResult 等）
    - ports.rs（Storage、ConfigProvider、Pipeline 等 trait）
    - services/
      - etl_engine.rs（EtlEngine，只依賴 domain 的 ports）
  - adapters/（適配層：外部依賴的具體實作，實作 domain ports）
    - config/
      - cli.rs、lambda.rs、toml.rs、sequence.rs（由 config/* 對應；名稱更語意化）
      - mod.rs
    - storage/
      - local.rs（原 LocalStorage）
      - s3.rs（Lambda feature 下的 S3Storage）
      - mod.rs
    - http/
      - client.rs（封裝 reqwest、重試、header、參數處理）
      - mod.rs
  - utils/
    - error.rs、logger.rs、monitor.rs、validation.rs、mod.rs（維持不變，或部分移入 adapters）
  - lib.rs（根據新結構調整對外 re-export）
  - bin/（若仍需要額外示例的 binary）
    - sequence_etl.rs、toml_etl.rs（也可改放 examples/）

- 根目錄新增：
  - examples/（取代部分 bin 範例，方便 `cargo run --example`）
  - docs/（持續性設計與決策記錄，本文件所在）

命名與一致性：
- 建議後續將 crate 名由「samll-etl」更正為「small-etl」（非本階段必做）。

---

## 三、檔案對應與遷移映射

- src/main.rs → src/app/cli/main.rs
- src/lambda.rs → src/app/lambda/main.rs（保留 [[bin]] 指向或改為標準 bin：src/bin/lambda.rs）
- src/core/
  - mod.rs → 拆分為 domain/model.rs、domain/ports.rs（保留 re-export）
  - etl.rs → domain/services/etl_engine.rs
  - pipeline.rs、mvp_pipeline.rs、pipeline_sequence.rs、contextual_pipeline.rs → 移至 app/pipelines/（若含 infra 細節，再局部下沉至 adapters）
- src/config/* → 移至 adapters/config/*（並按語意更名 toml.rs / sequence.rs）
- src/utils/* → 可維持；若明顯屬於 infra（如 logger 初始化），可移至 adapters/common/* 或在 app 層做薄封装
- src/bin/* → 視用途改放 examples/ 或保留在 bin/

---

## 四、增量遷移步驟（避免中斷編譯）

1. 僅新增資料夾與模組骨架（不移動原檔）：
   - 建立 app/、domain/、adapters/ 子資料夾與對應 mod 檔
   - 在 lib.rs 內先行導出新模組路徑（暫時空實作）
2. 拆出 domain：
   - 將 Record、TransformResult、Storage、ConfigProvider、Pipeline 等 trait 搬至 domain/
   - 調整引用路徑與 use 區塊，確保編譯通過
3. 拆出 services：
   - 將 EtlEngine 搬至 domain/services/etl_engine.rs，僅依賴 domain::ports
4. 調整 pipelines：
   - 將 pipeline.rs 等移至 app/pipelines/，把任何 http/storage 細節注入為 domain::ports 的實作
5. 整理 adapters：
   - 將 config/*、storage（Local/S3）、http client 放入 adapters，實作 domain::ports
6. 最後調整入口：
   - 把 CLI 與 Lambda 入口遷入 app/cli、app/lambda；視需要調整 Cargo.toml（[[bin]]] 或 features）

每步完成後都可編譯運行，降低風險。

---

## 五、對未來擴充的好處

- 新增資料來源（HTTP、檔案、DB）或新存儲（本地、S3、GCS）時，只需新增對應 adapters，domain 與 pipelines 無需變更
- 減少 feature gate 對內部模組的滲透，lambda/cli 的差異限定在 app 層與 adapters 層
- 測試更聚焦：
  - domain 可做純單元測試（無 IO）
  - adapters 可用整合測試或 mock（如 httpmock）
  - pipelines 可做端到端流程測試

---

## 六、短期可以先做的微調（零風險）

- 新增 examples/ 目錄，把教學/示例從 bin/ 遷過去（保留原 bin/ 不刪除，逐步切換）
- 在 docs/ 補充各 Pipeline 的責任與輸入/輸出定義，便於後續 domain 化
- 在 utils/ 中將 logger 初始化與格式設定做薄封裝，讓 app/cli 與 app/lambda 可共用

---

## 七、後續確認

若方案 OK，我們可依「增量遷移步驟」逐步提交 PR：
- PR1：新增骨架（app/domain/adapters 模組與 docs），不移動任何現有檔案
- PR2：抽離 domain 模型與 ports（確保編譯）
- PR3：搬遷 EtlEngine 至 domain/services
- PR4：逐一遷移 pipelines 與 adapters
- PR5：整理入口與 Cargo 配置

以上為第一階段（資料夾架構）之重構建議，請審閱後指示是否進入實作階段。
