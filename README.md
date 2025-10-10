# C-SQL - 簡易 SQLite Clone

一個使用 C 語言實作的輕量級關聯式資料庫，採用 B-tree 資料結構來儲存與查詢資料。本專案實現了基本的資料庫核心功能，包括持久化存儲、B-tree 索引、以及簡單的 SQL 語句支援。

## 📋 目錄

- [功能特色](#功能特色)
- [系統架構](#系統架構)
- [安裝與編譯](#安裝與編譯)
- [使用方式](#使用方式)
- [支援的指令](#支援的指令)
- [資料結構](#資料結構)
- [內部實作](#內部實作)
- [測試](#測試)
- [限制與已知問題](#限制與已知問題)
- [未來規劃](#未來規劃)

## ✨ 功能特色

- **B-tree 資料結構**：使用 B-tree 實現高效的資料存取
- **持久化存儲**：資料持久化保存至磁碟，支援跨 Session 存取
- **頁面管理系統**：實現 Pager 來管理記憶體與磁碟 I/O
- **SQL 語句支援**：支援基本的 INSERT、SELECT 和 UPDATE 操作
- **互動式 REPL**：提供命令列介面進行資料操作
- **除錯工具**：內建 B-tree 視覺化與常數查看功能
- **自動節點分裂**：當節點滿時自動進行分裂操作

## 🏗 系統架構

專案主要由以下四個核心組件構成：

### 1. Pager（頁面管理器）
- 負責檔案 I/O 操作
- 管理頁面快取（最多 100 頁）
- 處理頁面的讀取與寫入
- 頁面大小：4096 bytes

### 2. B-Tree（平衡樹）
- **葉節點**：儲存實際的資料列
- **內部節點**：作為索引，指向子節點
- 支援自動分裂與平衡
- 維護節點間的鏈結關係

### 3. Cursor（游標）
- 提供資料的遍歷功能
- 支援資料定位
- 追蹤當前位置狀態

### 4. REPL（讀取-求值-輸出循環）
- 互動式命令列介面
- 解析並執行使用者輸入
- 提供即時回饋

## 🔧 安裝與編譯

### 系統需求

- GCC 編譯器（支援 C11 標準）
- POSIX 相容系統（Linux、macOS、WSL）
- Python 3.x（用於執行測試腳本）

### 編譯步驟

```bash
# 進入專案目錄
cd c_sql

# 編譯程式
gcc -std=c11 -Wall -Wextra -O2 main.c -o main

# 或使用除錯模式編譯
gcc -std=c11 -Wall -Wextra -g main.c -o main
```

### 編譯選項說明

- `-std=c11`：使用 C11 標準
- `-Wall -Wextra`：啟用所有警告訊息
- `-O2`：啟用編譯最佳化（release 版本）
- `-g`：包含除錯資訊（debug 版本）

## 🚀 使用方式

### 啟動資料庫

```bash
# 啟動並指定資料庫檔案
./main mydb.db
```

如果資料庫檔案不存在，程式會自動建立一個新的資料庫。

### 基本操作範例

```sql
db > insert 1 user1 user1@example.com
Executed.

db > insert 2 user2 user2@example.com
Executed.

db > select
(1, user1, user1@example.com)
(2, user2, user2@example.com)
Executed.

db > update 1 updated_user updated@example.com
Executed.

db > select
(1, updated_user, updated@example.com)
(2, user2, user2@example.com)
Executed.

db > .exit
```

## 📖 支援的指令

### SQL 語句

#### INSERT
插入一筆新資料

```sql
insert [id] [username] [email]
```

**範例：**
```sql
db > insert 1 john john@example.com
Executed.
```

**限制：**
- ID 必須為正整數
- Username 最長 32 字元
- Email 最長 255 字元
- ID 不可重複（會回傳 "Error: Duplicate key."）

#### SELECT
查詢並顯示所有資料

```sql
select
```

**範例：**
```sql
db > select
(1, john, john@example.com)
(2, mary, mary@example.com)
Executed.
```

#### UPDATE
更新指定 ID 的資料

```sql
update [id] [username] [email]
```

**範例：**
```sql
db > update 1 john_updated new_email@example.com
Executed.
```

**限制：**
- ID 必須為正整數
- Username 最長 32 字元
- Email 最長 255 字元
- 若指定的 ID 不存在，會回傳 "Error: Key not found."

**注意：** UPDATE 語句會完全替換指定 ID 的 username 和 email，而不是部分更新。

### 元命令（Meta Commands）

元命令以 `.` 開頭，用於資料庫管理與除錯。

#### .exit
結束程式並關閉資料庫

```bash
db > .exit
```

#### .btree
顯示 B-tree 的樹狀結構，用於視覺化與除錯

```bash
db > .btree
Tree:
- internal (size 1)
  - leaf (size 7)
    - 1
    - 2
    - 3
    - 4
    - 5
    - 6
    - 7
  - key 7
  - leaf (size 3)
    - 8
    - 9
    - 10
```

#### .constants
顯示系統內部常數，用於開發與除錯

```bash
db > .constants
Constants:
ROW_SIZE: 293
COMMON_NODE_HEADER_SIZE: 6
LEAF_NODE_HEADER_SIZE: 14
LEAF_NODE_CELL_SIZE: 297
LEAF_NODE_SPACE_FOR_CELLS: 4082
LEAF_NODE_MAX_CELLS: 13
INTERNAL_NODE_HEADER_SIZE: 14
INTERNAL_NODE_CELL_SIZE: 8
INTERNAL_NODE_MAX_CELLS: 3
```

## 📊 資料結構

### Row（資料列）

每筆資料包含三個欄位：

| 欄位 | 類型 | 大小 | 說明 |
|------|------|------|------|
| id | uint32_t | 4 bytes | 主鍵，唯一識別碼 |
| username | char[33] | 33 bytes | 使用者名稱 |
| email | char[256] | 256 bytes | 電子郵件地址 |

**總大小：** 293 bytes

### B-Tree 節點結構

#### 共同標頭（6 bytes）
- `node_type` (1 byte)：節點類型（葉節點或內部節點）
- `is_root` (1 byte)：是否為根節點
- `parent` (4 bytes)：父節點的頁面編號

#### 葉節點
```
[共同標頭 6B] [num_cells 4B] [next_leaf 4B] [Cell 1] [Cell 2] ... [Cell N]
```

每個 Cell 包含：
- Key (4 bytes)：主鍵
- Value (293 bytes)：完整的 Row 資料

**葉節點最大容量：** 13 筆資料

#### 內部節點
```
[共同標頭 6B] [num_keys 4B] [right_child 4B] [Cell 1] [Cell 2] ... [Cell N]
```

每個 Cell 包含：
- Child (4 bytes)：子節點的頁面編號
- Key (4 bytes)：該子節點的最大鍵值

**內部節點最大鍵數：** 3（設定為較小值以便測試）

## 🔍 內部實作

### 頁面管理

- **頁面大小：** 4096 bytes
- **最大頁數：** 100 頁
- **總容量：** 約 409,600 bytes

### B-Tree 操作

#### 插入流程
1. 使用 `table_find()` 定位插入位置
2. 檢查是否有重複鍵值
3. 如果葉節點未滿，直接插入
4. 如果葉節點已滿，執行節點分裂
5. 更新父節點的索引

#### 節點分裂
當葉節點達到最大容量（13 筆）時：
1. 建立新的葉節點
2. 將原節點的資料分成兩半
3. 左節點保留 7 筆資料
4. 右節點移入 7 筆資料
5. 更新父節點或建立新的根節點

#### 查詢流程
1. 從根節點開始
2. 如果是內部節點，找到對應的子節點
3. 重複步驟 2 直到到達葉節點
4. 在葉節點中搜尋資料

### 持久化機制

- 資料以頁面為單位寫入磁碟
- 使用 `pager_flush()` 將髒頁寫回檔案
- 關閉資料庫時自動同步所有頁面
- 支援跨 Session 的資料持久性

## 🧪 測試

專案包含 Python 測試腳本 `test_db.py`，用於自動化測試。

### 執行測試

```bash
# 編譯程式
gcc -std=c11 -Wall -Wextra main.c -o main

# 執行測試腳本
python3 test_db.py
```

### 測試內容

測試腳本包含兩個測試場景：

1. **第一次 Session：**
   - 插入 86 筆測試資料
   - 觸發多次節點分裂
   - 驗證 B-tree 結構
   - 查詢所有資料

2. **第二次 Session：**
   - 重新開啟同一個資料庫檔案
   - 驗證資料持久性
   - 確認資料完整性

### 手動測試範例

```bash
# 測試基本插入與查詢
./main test.db << EOF
insert 1 user1 user1@example.com
insert 2 user2 user2@example.com
insert 3 user3 user3@example.com
select
.exit
EOF

# 測試持久化
./main test.db << EOF
select
.exit
EOF

# 測試重複鍵值
./main test.db << EOF
insert 1 duplicate duplicate@example.com
.exit
EOF

# 查看 B-tree 結構
./main test.db << EOF
.btree
.exit
EOF
```

## ⚠️ 限制與已知問題

### 功能限制

1. **不支援 DELETE 語句**：無法刪除已插入的資料
2. **不支援 WHERE 子句**：SELECT 只能查詢所有資料，UPDATE 只能透過主鍵更新
3. **固定的資料結構**：欄位類型和數量固定
4. **無索引支援**：除了主鍵外沒有其他索引
5. **無交易支援**：不支援 ACID 特性
6. **無並發控制**：不支援多使用者同時存取
7. **UPDATE 限制**：UPDATE 只能完全替換整筆記錄，無法部分更新特定欄位

### 容量限制

- **最大頁數：** 100 頁
- **每頁大小：** 4096 bytes
- **葉節點容量：** 每個葉節點最多 13 筆資料
- **理論最大資料筆數：** 約 1,300 筆（100 頁 × 13 筆）
- **Username：** 最長 32 字元
- **Email：** 最長 255 字元

### 已知問題

1. 內部節點最大鍵數設為 3，主要用於測試，可能影響大量資料的效能
2. 頁面快取沒有 LRU 或其他置換策略
3. 錯誤處理不夠完善
4. 缺少輸入驗證（如 SQL injection 防護）

## 🚧 未來規劃

### 已完成

- [x] 實作 UPDATE 語句（2025-10-10）

### 開發中

- [ ] 實作 DELETE 語句
- [ ] 支援 WHERE 子句篩選
- [ ] 改善 UPDATE 語句支援部分欄位更新
- [ ] 改善錯誤處理與訊息
- [ ] 增加更多的測試案例

### 長期目標

- [ ] 支援多欄位索引
- [ ] 實作 JOIN 操作
- [ ] 支援更多資料類型（INT, FLOAT, TEXT, DATE）
- [ ] 實作基本的查詢最佳化
- [ ] 增加頁面快取置換策略
- [ ] 實作交易支援（ACID）
- [ ] 並發控制（鎖機制）
- [ ] 查詢計畫與最佳化器
- [ ] 支援 VIEW 和 TRIGGER
- [ ] 網路協議支援（Client-Server 架構）

## 📚 學習資源

本專案的實作參考了以下資源：

- [Let's Build a Simple Database](https://cstack.github.io/db_tutorial/)


## 📝 程式碼結構

```
c_sql/
├── main.c          # 主程式（1463 行）
├── save_main.c     # 備份檔案
├── test_db.py      # Python 測試腳本
└── README.md       # 本文件
```


## 📄 授權

本專案為教育用途，歡迎自由使用與修改。

## 👤 作者
paulboul1013
本專案為學習資料庫內部實作的練習專案。

---

**最後更新：** 2025-10-10

