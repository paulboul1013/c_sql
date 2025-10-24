# C-SQL - 簡易 SQLite Clone

一個使用 C 語言實作的輕量級關聯式資料庫，採用 B-tree 資料結構來儲存與查詢資料。本專案實現了基本的資料庫核心功能，包括持久化存儲、B-tree 索引、以及簡單的 SQL 語句支援。

##  目錄

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

## 功能特色

- **B-tree 資料結構**：使用 B-tree 實現高效的資料存取
- **持久化存儲**：資料持久化保存至磁碟，支援跨 Session 存取
- **頁面管理系統**：實現 Pager 來管理記憶體與磁碟 I/O
- **SQL 語句支援**：支援基本的 INSERT、SELECT、UPDATE 和 DELETE 操作
- **互動式 REPL**：提供命令列介面進行資料操作
- **除錯工具**：內建 B-tree 視覺化與常數查看功能
- **自動節點分裂**：當節點滿時自動進行分裂操作

##  系統架構

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

##  安裝與編譯

### 系統需求

- GCC 編譯器（支援 C11 標準）
- POSIX 相容系統（Linux、macOS、WSL）
- Python 3.x（用於執行測試腳本）

### 編譯步驟

#### 使用 Makefile（推薦）

```bash
# 進入專案目錄
cd c_sql

# 編譯程式
make

# 編譯 Debug 版本
make debug

# 顯示所有可用命令
make help
```

#### 手動編譯

```bash
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

##  使用方式

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

db > insert 3 admin admin@example.com
Executed.

db > select
(1, user1, user1@example.com)
(2, user2, user2@example.com)
(3, admin, admin@example.com)
Executed.

db > select where id = 2
(2, user2, user2@example.com)
Executed.

db > select where id > 1
(2, user2, user2@example.com)
(3, admin, admin@example.com)
Executed.

db > update 1 updated_user updated@example.com
Executed.

db > update - newadmin@example.com where username = admin
Executed.

db > select
(1, updated_user, updated@example.com)
(2, user2, user2@example.com)
(3, admin, newadmin@example.com)
Executed.

db > delete where id = 2
Executed.

db > select
(1, updated_user, updated@example.com)
(3, admin, newadmin@example.com)
Executed.

db > .exit
```

##   支援的指令

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
查詢並顯示資料，支援 WHERE 子句篩選

```sql
select
select where [condition]
```

**範例：**
```sql
# 查詢所有資料
db > select
(1, john, john@example.com)
(2, mary, mary@example.com)
Executed.

# 使用 WHERE 子句篩選
db > select where id = 1
(1, john, john@example.com)
Executed.

db > select where username = john
(1, john, john@example.com)
Executed.

db > select where id > 1
(2, mary, mary@example.com)
Executed.

db > select where id > 1 AND id < 5
(2, mary, mary@example.com)
(3, john, john@example.com)
(4, alice, alice@example.com)
Executed.

db > select where username = john OR username = alice
(1, john, john@example.com)
(4, alice, alice@example.com)
Executed.
```

**WHERE 子句支援：**
- 欄位：`id`、`username`、`email`
- 運算符：`=`、`!=`、`>`、`<`、`>=`、`<=`
- 邏輯運算符：`AND`、`OR`（支援複雜條件組合）
- **括號優先級：**支援使用括號改變運算符的優先級
- 語法：
  - 單一條件：`field operator value`
  - 複雜條件：`field operator value AND/OR field operator value ...`
  - 括號表達式：`(field operator value AND/OR ...) AND/OR ...`
- 範例：
  - `id = 5`（單一條件）
  - `id > 2 AND id < 10`（AND 條件）
  - `username = admin OR username = root`（OR 條件）
  - `(id < 5 OR id > 10) AND username != user`（使用括號改變優先級）
  - `(id < 3 OR id > 15) AND (username = alice OR username = root)`（多個括號組合）
  - `((id < 3 OR id > 15) AND username = alice) OR username = bob`（嵌套括號）

#### UPDATE
更新資料（支援部分欄位更新和 WHERE 子句）

```sql
update [id] [username] [email]
update [username] [email] where [condition]
```

**範例：**
```sql
# 舊式語法：透過 ID 更新
db > update 1 john_updated new_email@example.com
Executed.

# 只更新 email（使用 '-' 表示不更新 username）
db > update 1 - another_email@example.com
Executed.

# 只更新 username（使用 '-' 表示不更新 email）
db > update 1 jane_updated -
Executed.

# 使用 WHERE 子句更新
db > update new_name new@email.com where id = 1
Executed.

db > update - updated@email.com where username = john
Executed.

db > update new_user - where id > 5
Executed.
```

**限制：**
- ID 必須為正整數
- Username 最長 32 字元
- Email 最長 255 字元
- 若找不到符合條件的資料，會回傳 "Error: Key not found."

**部分欄位更新：** 使用 `-` 表示不更新該欄位，只更新指定的欄位，其他欄位保持原值不變。

**WHERE 子句支援：**
- 欄位：`id`、`username`、`email`
- 運算符：`=`、`!=`、`>`、`<`、`>=`、`<=`
- 邏輯運算符：`AND`、`OR`（支援複雜條件組合）
- **括號優先級：**支援使用括號改變運算符的優先級
- 語法：與 SELECT 相同，支援單一條件、複雜條件組合和括號表達式

#### DELETE
刪除資料（支援 WHERE 子句）

```sql
delete [id]
delete where [condition]
```

**範例：**
```sql
# 舊式語法：透過 ID 刪除
db > delete 5
Executed.

# 使用 WHERE 子句刪除
db > delete where id = 5
Executed.

db > delete where username = john
Executed.

db > delete where id > 10
Executed.
```

**限制：**
- ID 必須為正整數
- 若找不到符合條件的資料，會回傳 "Error: Key not found."
- 批次刪除最多支援 1000 筆資料

**WHERE 子句支援：**
- 欄位：`id`、`username`、`email`
- 運算符：`=`、`!=`、`>`、`<`、`>=`、`<=`
- 邏輯運算符：`AND`、`OR`（支援複雜條件組合）
- **括號優先級：**支援使用括號改變運算符的優先級
- 語法：與 SELECT 相同，支援單一條件、複雜條件組合和括號表達式
- 範例：
  - `delete where id = 5`（單一條件）
  - `delete where id > 10 AND id < 20`（AND 條件）
  - `delete where username = test OR username = demo`（OR 條件）
  - `delete where (id = 3 OR id = 4) AND username != admin`（使用括號改變優先級）

**注意：** DELETE 操作會將資料從 B-tree 中完全移除。刪除操作會將葉節點中的資料向前移動以填補空缺，並減少節點的 cell 數量。當使用 WHERE 子句批次刪除時，程式會先收集所有符合條件的 ID，然後從後往前逐一刪除。

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

##  資料結構

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

##  內部實作

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

#### 刪除流程
1. 使用 `table_find()` 定位要刪除的鍵值
2. 檢查鍵值是否存在
3. 如果找到，從葉節點中移除該 cell
4. 將後續的 cell 向前移動以填補空缺
5. 減少葉節點的 cell 數量

**注意：** 已實作節點合併機制，當葉節點變為空節點時會自動與兄弟節點合併，提高空間利用率。

#### 節點分裂
當葉節點達到最大容量（13 筆）時：
1. 建立新的葉節點
2. 將原節點的資料分成兩半
3. 左節點保留 7 筆資料
4. 右節點移入 7 筆資料
5. 更新父節點或建立新的根節點

#### 節點合併
當葉節點變為空節點時：
1. 檢查是否可以與左兄弟節點合併
2. 檢查是否可以與右兄弟節點合併
3. 將空節點的資料合併到兄弟節點
4. 從父節點中移除空節點的引用
5. 釋放空節點的頁面空間

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

##  測試

專案包含 Python 測試腳本 `test_db.py`，用於自動化測試。

### 執行測試

```bash
# 編譯程式
gcc -std=c11 -Wall -Wextra main.c -o main

# 執行 Python 測試腳本
python3 test_db.py
```

### 測試內容

測試腳本包含兩個測試場景：

1. **基本 CRUD 操作**：INSERT、SELECT、UPDATE、DELETE 基本功能測試
2. **節點分裂功能**：插入大量資料觸發 B-tree 節點分裂
3. **節點合併功能**：刪除資料觸發節點合併機制
4. **錯誤處理**：重複鍵值、不存在的資料等錯誤情況處理
5. **資料持久化**：跨 Session 的資料持久性驗證
6. **大量資料處理**：100+ 筆資料的處理能力測試
7. **WHERE 子句功能**：完整的 WHERE 條件測試（`=`, `!=`, `>`, `<`, `>=`, `<=`）
8. **WHERE 邊界情況**：空結果集、不存在的資料、字串比較等
9. **WHERE 效能測試**：大量資料下的 WHERE 查詢效能
10. **複雜操作組合**：多種操作混合執行的測試

### 手動測試範例

```bash
# 測試基本插入與查詢
./main test.db 
insert 1 user1 user1@example.com
insert 2 user2 user2@example.com
insert 3 user3 user3@example.com
select
.exit
EOF

# 測試持久化
./main test.db 
select
.exit
EOF

# 測試重複鍵值
./main test.db 
insert 1 duplicate duplicate@example.com
.exit
EOF

# 測試刪除操作
./main test.db 
delete 2
select
.exit
EOF

# 測試刪除不存在的鍵值
./main test.db 
delete 999
.exit
EOF

# 查看 B-tree 結構
./main test.db 
.btree
.exit
EOF
```

## 限制與已知問題

### 功能限制

1. **WHERE 子句限制**：
   - 支援 AND、OR 邏輯運算，並支援括號改變優先級
   - 最多支援 30 個表達式節點（每個基本條件或邏輯運算都算一個節點）
2. **固定的資料結構**：欄位類型和數量固定
3. **無索引支援**：除了主鍵外沒有其他索引，WHERE 子句查詢需要全表掃描（除了 id = value 的情況）
4. **無交易支援**：不支援 ACID 特性
5. **無並發控制**：不支援多使用者同時存取
6. **批次刪除限制**：使用 WHERE 子句的批次刪除最多支援 1000 筆資料

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
5. DELETE 操作後節點不會自動合併，可能造成空間浪費

## 未來規劃

### 已完成

- [x] 實作 UPDATE 語句（2025-10-10）
- [x] 實作 DELETE 語句（2025-10-13）
- [x] 實作 DELETE 操作的節點合併機制（2025-10-14）
- [x] 改善 UPDATE 語句支援部分欄位更新（2025-10-16）
- [x] 支援 WHERE 子句篩選（2025-10-17）
- [x] 支援 WHERE 子句的複雜條件（AND、OR）（2025-10-23）
- [x] 支援 WHERE 子句的括號優先級（2025-10-24）

### 開發中

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

##  學習資源

本專案的實作參考了以下資源：

- [Let's Build a Simple Database](https://cstack.github.io/db_tutorial/)


## 程式碼結構

```
c_sql/
├── main.c          # 主程式（C 語言）
├── Makefile        # 編譯與測試腳本
├── test_db.py      # Python 測試腳本（完整測試套件）
└── README.md       # 本文件
```


##  作者
paulboul1013
本專案為學習資料庫內部實作的練習專案。

---

**最後更新：** 2025-10-24

