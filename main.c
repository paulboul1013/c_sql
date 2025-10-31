/*
 * ============================================================================
 * Simple SQLite Clone - B-Tree Based Database Implementation
 * ============================================================================
 *
 * 這是一個簡易的關聯式資料庫實作，使用 B-tree 資料結構來儲存與查詢資料。
 * 支援基本的 INSERT 和 SELECT 指令。
 *
 * 主要組成部分：
 * 1. Pager：管理檔案 I/O 與頁面快取
 * 2. B-Tree：使用葉節點儲存資料，內部節點作為索引
 * 3. Cursor：提供資料的遍歷與定位
 * 4. REPL：互動式命令列介面
 *
 * ============================================================================
 */

#define _POSIX_C_SOURCE 200809L

#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <math.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

/* ============================================================================
 * 常數定義
 * ============================================================================
 */

#define TABLE_MAX_PAGES 100
#define INVALID_PAGE_NUM UINT32_MAX
#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255

/* ============================================================================
 * 型別定義與資料結構
 * ============================================================================
 */

// B-tree 節點類型
typedef enum {
  NODE_INTERNAL, // 內部節點（索引）
  NODE_LEAF      // 葉節點（資料）
} NodeType;

// 執行結果狀態
typedef enum {
  EXECUTE_SUCCESS,
  EXECUTE_DUPLICATE_KEY,
  EXECUTE_TABLE_FULL,
  EXECUTE_KEY_NOT_FOUND
} ExecuteResult;

// 查詢計畫類型
typedef enum {
  QUERY_PLAN_FULL_SCAN,      // 全表掃描
  QUERY_PLAN_INDEX_LOOKUP,   // 索引查找（id = value）
  QUERY_PLAN_RANGE_SCAN      // 範圍掃描（id > value 或 id < value）
} QueryPlanType;

// 查詢計畫
typedef struct {
  QueryPlanType type;
  uint32_t start_key;  // 範圍掃描的起始鍵
  bool has_start_key;  // 是否有起始鍵
  bool forward;        // 是否正向掃描
  double estimated_cost;  // 估算的成本
  uint32_t estimated_rows; // 估算的結果行數
} QueryPlan;

// 表統計資訊
typedef struct {
  uint32_t total_rows;        // 總行數
  uint32_t id_min;            // ID 最小值
  uint32_t id_max;            // ID 最大值
  uint32_t id_cardinality;    // ID 欄位的基數（不同值的數量）
  uint32_t username_cardinality; // Username 欄位的基數
  uint32_t email_cardinality;   // Email 欄位的基數
  bool is_valid;              // 統計資訊是否有效
} TableStatistics;

// SQL 語句類型
typedef enum {
  STATEMENT_INSERT,
  STATEMENT_SELECT,
  STATEMENT_UPDATE,
  STATEMENT_DELETE
} StatementType;

// WHERE 子句的欄位類型
typedef enum {
  WHERE_FIELD_NONE, // 沒有 WHERE 子句
  WHERE_FIELD_ID,
  WHERE_FIELD_USERNAME,
  WHERE_FIELD_EMAIL
} WhereFieldType;

// WHERE 子句的比較運算符
typedef enum {
  WHERE_OP_EQUAL,         // =
  WHERE_OP_NOT_EQUAL,     // !=
  WHERE_OP_GREATER,       // >
  WHERE_OP_LESS,          // <
  WHERE_OP_GREATER_EQUAL, // >=
  WHERE_OP_LESS_EQUAL     // <=
} WhereOperator;

// WHERE 子句的邏輯運算符
typedef enum {
  WHERE_LOGICAL_NONE, // 沒有邏輯運算符
  WHERE_LOGICAL_AND,  // AND
  WHERE_LOGICAL_OR    // OR
} WhereLogicalOp;

// 單一 WHERE 條件（基本條件）
typedef struct {
  WhereFieldType field;
  WhereOperator op;
  union {
    uint32_t id_value;
    char string_value[256];
  } value;
} WhereBasicCondition;

// WHERE 表達式節點類型
typedef enum {
  WHERE_EXPR_BASIC,  // 基本條件
  WHERE_EXPR_AND,    // AND 運算
  WHERE_EXPR_OR      // OR 運算
} WhereExprType;

// WHERE 表達式節點（使用陣列索引代替指標）
#define MAX_WHERE_EXPR_NODES 30
#define INVALID_EXPR_INDEX 0xFFFFFFFF
typedef struct {
  WhereExprType type;
  union {
    WhereBasicCondition basic;  // 基本條件
    struct {
      uint32_t left;   // 左子表達式索引
      uint32_t right;  // 右子表達式索引
    } logical;
  } data;
} WhereExprNode;

// WHERE 條件（支援多個條件組合和括號）
#define MAX_WHERE_CONDITIONS 10
typedef struct {
  WhereFieldType field; // 保持向後兼容
  WhereOperator op;     // 保持向後兼容
  union {               // 保持向後兼容
    uint32_t id_value;
    char string_value[256];
  } value;
  // 新增：支援多個條件
  uint32_t num_conditions;                           // 條件數量
  WhereBasicCondition conditions[MAX_WHERE_CONDITIONS]; // 條件陣列
  WhereLogicalOp logical_ops[MAX_WHERE_CONDITIONS - 1]; // 邏輯運算符陣列
  // 新增：支援括號優先級的表達式樹
  WhereExprNode expr_nodes[MAX_WHERE_EXPR_NODES];  // 表達式節點陣列
  uint32_t num_expr_nodes;  // 表達式節點數量
  uint32_t root_expr;       // 根表達式索引
  bool use_expr_tree;       // 是否使用表達式樹
} WhereCondition;

// 元命令執行結果
typedef enum {
  META_COMMAND_SUCCESS,
  META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

// 語句解析結果
typedef enum {
  PREPARE_SUCCESS,
  PREPARE_NEGATIVE_ID,
  PREPARE_STRING_TOO_LONG,
  PREPARE_SYNTAX_ERROR,
  PREPARE_UNRECOGNIZED_STATEMENT
} PrepareResult;

// 輸入緩衝區
typedef struct {
  char *buffer;
  size_t buffer_length;
  ssize_t input_length;
} InputBuffer;

// 資料列結構
typedef struct {
  uint32_t id;
  char username[COLUMN_USERNAME_SIZE + 1];
  char email[COLUMN_EMAIL_SIZE + 1];
} Row;

// SQL 語句結構
typedef struct {
  StatementType type;
  Row row_to_insert; // 用於 INSERT、UPDATE 和 DELETE (DELETE 只使用 id 欄位)
  // 用於 UPDATE 語句的欄位更新標識
  bool update_username; // 是否更新 username
  bool update_email;    // 是否更新 email
  // WHERE 條件
  WhereCondition where; // WHERE 子句條件
} Statement;

// 頁面管理器
typedef struct {
  int file_descriptor;
  uint32_t file_length;
  uint32_t num_pages;
  void *pages[TABLE_MAX_PAGES];
} Pager;

// 交易狀態
typedef enum {
  TXN_STATE_NONE,       // 沒有交易
  TXN_STATE_ACTIVE,     // 交易進行中
  TXN_STATE_COMMITTED,  // 已提交
  TXN_STATE_ABORTED     // 已中止
} TransactionState;

// 交易結構（使用 Shadow Paging）
typedef struct {
  TransactionState state;
  void *shadow_pages[TABLE_MAX_PAGES];  // 影子頁面（交易中修改的頁面副本）
  bool modified_pages[TABLE_MAX_PAGES]; // 標記哪些頁面被修改過
  uint32_t num_modified;                // 被修改的頁面數量
} Transaction;

// 資料表結構
typedef struct {
  Pager *pager;
  uint32_t root_page_num;
  Transaction *transaction;  // 當前交易
  TableStatistics *statistics; // 統計資訊
} Table;

// Cursor：用於遍歷與定位資料
typedef struct {
  Table *table;
  uint32_t page_num;
  uint32_t cell_num;
  bool end_of_table;
} Cursor;

/* ============================================================================
 * Row 序列化常數
 * ============================================================================
 */

#define size_of_attribute(Struct, Attribute) sizeof(((Struct *)0)->Attribute)

const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

const uint32_t PAGE_SIZE = 4096;

/* ============================================================================
 * B-Tree 節點佈局常數
 * ============================================================================
 */

/*
 * 所有節點的共同標頭（Common Node Header）：
 * - node_type: 節點類型（葉節點或內部節點）
 * - is_root: 是否為根節點
 * - parent: 父節點的頁面編號
 */
const uint32_t NODE_TYPE_SIZE = sizeof(uint8_t);
const uint32_t NODE_TYPE_OFFSET = 0;
const uint32_t IS_ROOT_SIZE = sizeof(uint8_t);
const uint32_t IS_ROOT_OFFSET = NODE_TYPE_SIZE;
const uint32_t PARENT_POINTER_SIZE = sizeof(uint32_t);
const uint32_t PARENT_POINTER_OFFSET = IS_ROOT_SIZE + IS_ROOT_OFFSET;
const uint32_t COMMON_NODE_HEADER_SIZE =
    NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

/*
 * 葉節點標頭（Leaf Node Header）：
 * - num_cells: 儲存的資料筆數
 * - next_leaf: 下一個葉節點的頁面編號（用於順序遍歷）
 */
const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_NEXT_LEAF_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NEXT_LEAF_OFFSET =
    LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE;
const uint32_t LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE +
                                       LEAF_NODE_NUM_CELLS_SIZE +
                                       LEAF_NODE_NEXT_LEAF_SIZE;

/*
 * 葉節點 Cell 佈局：
 * - key: 主鍵
 * - value: 完整的 Row 資料
 */
const uint32_t LEAF_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_KEY_OFFSET = 0;
const uint32_t LEAF_NODE_VALUE_SIZE = ROW_SIZE;
const uint32_t LEAF_NODE_VALUE_OFFSET =
    LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const uint32_t LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const uint32_t LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_MAX_CELLS =
    LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

// 葉節點分裂時的分配數量
const uint32_t LEAF_NODE_RIGHT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) / 2;
const uint32_t LEAF_NODE_LEFT_SPLIT_COUNT =
    (LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_RIGHT_SPLIT_COUNT;

/*
 * 內部節點標頭（Internal Node Header）：
 * - num_keys: 鍵的數量
 * - right_child: 最右側子節點的頁面編號
 */
const uint32_t INTERNAL_NODE_NUM_KEYS_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_NUM_KEYS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t INTERNAL_NODE_RIGHT_CHILD_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_RIGHT_CHILD_OFFSET =
    INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE;
const uint32_t INTERNAL_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE +
                                           INTERNAL_NODE_NUM_KEYS_SIZE +
                                           INTERNAL_NODE_RIGHT_CHILD_SIZE;

/*
 * 內部節點 Cell 佈局：
 * - child: 子節點的頁面編號
 * - key: 該子節點的最大鍵值
 */
const uint32_t INTERNAL_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_CHILD_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_CELL_SIZE =
    INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE;

// 為了測試方便，暫時設定較小的值
const uint32_t INTERNAL_NODE_MAX_CELLS = 3;

/* ============================================================================
 * 函式前置宣告
 * ============================================================================
 */

NodeType get_node_type(void *node);
void set_node_type(void *node, NodeType type);
bool is_node_root(void *node);
void set_node_root(void *node, bool is_root);
uint32_t *node_parent(void *node);
void *get_page(Pager *pager, uint32_t page_num);
uint32_t get_unused_page_num(Pager *pager);
uint32_t get_node_max_key(Pager *pager, void *node);

// 葉節點操作
uint32_t *leaf_node_num_cells(void *node);
uint32_t *leaf_node_next_leaf(void *node);
void *leaf_node_cell(void *node, uint32_t cell_num);
uint32_t *leaf_node_key(void *node, uint32_t cell_num);
void *leaf_node_value(void *node, uint32_t cell_num);
void initialize_leaf_node(void *node);
void leaf_node_insert(Cursor *cursor, uint32_t key, Row *value);
void leaf_node_split_and_insert(Cursor *cursor, uint32_t key, Row *value);
void leaf_node_delete(Cursor *cursor);
void leaf_node_merge(Table *table, uint32_t left_page_num,
                     uint32_t right_page_num);
void internal_node_merge(Table *table, uint32_t parent_page_num,
                         uint32_t left_page_num, uint32_t right_page_num);
bool should_merge_leaf_nodes(Table *table, uint32_t page_num);
bool should_merge_internal_nodes(Table *table, uint32_t page_num);
Cursor *leaf_node_find(Table *table, uint32_t page_num, uint32_t key);

// 內部節點操作
uint32_t *internal_node_num_keys(void *node);
uint32_t *internal_node_right_child(void *node);
uint32_t *internal_node_child(void *node, uint32_t child_num);
uint32_t *internal_node_key(void *node, uint32_t key_num);
void *internal_node_cell(void *node, uint32_t cell_num);
void initialize_internal_node(void *node);
uint32_t internal_node_find_child(void *node, uint32_t key);
void internal_node_insert(Table *table, uint32_t parent_page_num,
                          uint32_t child_page_num);
void internal_node_split_and_insert(Table *table, uint32_t parent_page_num,
                                    uint32_t child_page_num);
void update_internal_node_key(void *node, uint32_t old_key, uint32_t new_key);
Cursor *internal_node_find(Table *table, uint32_t page_num, uint32_t key);

// B-tree 操作
Cursor *table_find(Table *table, uint32_t key);
Cursor *table_start(Table *table);
void create_new_root(Table *table, uint32_t right_child_page_num);

// Cursor 操作
void *cursor_value(Cursor *cursor);
void cursor_advance(Cursor *cursor);

// 序列化
void serialize_row(Row *source, void *destination);
void deserialize_row(void *source, Row *destination);

// 輸入與輸出
void print_prompt(void);
void read_input(InputBuffer *input_buffer);
void print_row(Row *row);
void print_constants(void);
void print_tree(Pager *pager, uint32_t page_num, uint32_t indentation_level);

// Pager 與資料庫管理
Pager *pager_open(const char *filename);
void pager_flush(Pager *pager, uint32_t page_num);
Table *db_open(const char *filename);
void db_close(Table *table);

// 交易管理
Transaction *transaction_begin(Table *table);
ExecuteResult transaction_commit(Table *table);
ExecuteResult transaction_rollback(Table *table);
void *get_page_for_read(Table *table, uint32_t page_num);
void *get_page_for_write(Table *table, uint32_t page_num);
bool is_in_transaction(Table *table);

// 命令處理
InputBuffer *new_input_buffer(void);
void close_input_buffer(InputBuffer *input_buffer);
MetaCommandResult do_meta_command(InputBuffer *input_buffer, Table *table);
PrepareResult prepare_statement(InputBuffer *input_buffer,
                                Statement *statement);
PrepareResult prepare_insert(InputBuffer *input_buffer, Statement *statement);
PrepareResult prepare_update(InputBuffer *input_buffer, Statement *statement);
PrepareResult prepare_delete(InputBuffer *input_buffer, Statement *statement);
PrepareResult parse_where_clause(char *where_clause, WhereCondition *where);
PrepareResult parse_where_expression(char **input, WhereCondition *where, uint32_t *expr_idx);
PrepareResult parse_where_or_expr(char **input, WhereCondition *where, uint32_t *expr_idx);
PrepareResult parse_where_and_expr(char **input, WhereCondition *where, uint32_t *expr_idx);
PrepareResult parse_where_primary_expr(char **input, WhereCondition *where, uint32_t *expr_idx);
PrepareResult parse_basic_condition(char **input, WhereBasicCondition *condition);
void skip_whitespace(char **input);
bool evaluate_basic_condition(Row *row, WhereBasicCondition *condition);
bool evaluate_where_condition(Row *row, WhereCondition *where);
bool evaluate_expr_tree(Row *row, WhereCondition *where, uint32_t expr_idx);
ExecuteResult execute_statement(Statement *statement, Table *table);
ExecuteResult execute_insert(Statement *statement, Table *table);
ExecuteResult execute_select(Statement *statement, Table *table);
ExecuteResult execute_update(Statement *statement, Table *table);
ExecuteResult execute_delete(Statement *statement, Table *table);
QueryPlan create_query_plan(WhereCondition *where);
QueryPlan create_query_plan_with_stats(WhereCondition *where, TableStatistics *stats);
double estimate_query_cost(QueryPlan *plan, TableStatistics *stats, WhereCondition *where);
uint32_t estimate_result_rows(QueryPlan *plan, TableStatistics *stats, WhereCondition *where);
TableStatistics *collect_table_statistics(Table *table);
void statistics_update_on_insert(TableStatistics *stats, Row *row);
void statistics_update_on_delete(TableStatistics *stats, Row *row);
bool statistics_load(Table *table);
bool statistics_save(Table *table);
void statistics_reset(TableStatistics *stats);

/* ============================================================================
 * 輔助與工具函式
 * ============================================================================
 */

/**
 * 印出縮排空格，用於樹狀結構的視覺化
 */
void indent(uint32_t level) {
  for (uint32_t i = 0; i < level; i++) {
    printf("  ");
  }
}

/**
 * 印出系統常數，用於除錯
 */
void print_constants(void) {
  printf("ROW_SIZE: %u\n", ROW_SIZE);
  printf("COMMON_NODE_HEADER_SIZE: %u\n", COMMON_NODE_HEADER_SIZE);
  printf("LEAF_NODE_HEADER_SIZE: %u\n", LEAF_NODE_HEADER_SIZE);
  printf("LEAF_NODE_CELL_SIZE: %u\n", LEAF_NODE_CELL_SIZE);
  printf("LEAF_NODE_SPACE_FOR_CELLS: %u\n", LEAF_NODE_SPACE_FOR_CELLS);
  printf("LEAF_NODE_MAX_CELLS: %u\n", LEAF_NODE_MAX_CELLS);
  printf("INTERNAL_NODE_HEADER_SIZE: %u\n", INTERNAL_NODE_HEADER_SIZE);
  printf("INTERNAL_NODE_CELL_SIZE: %u\n", INTERNAL_NODE_CELL_SIZE);
  printf("INTERNAL_NODE_MAX_CELLS: %u\n", INTERNAL_NODE_MAX_CELLS);
}

/**
 * 遞迴印出 B-tree 的結構，用於除錯與視覺化
 */
void print_tree(Pager *pager, uint32_t page_num, uint32_t indentation_level) {
  void *node = get_page(pager, page_num);
  uint32_t num_keys, child;

  switch (get_node_type(node)) {
  case NODE_LEAF:
    num_keys = *leaf_node_num_cells(node);
    indent(indentation_level);
    printf("- leaf (size %u)\n", num_keys);
    for (uint32_t i = 0; i < num_keys; i++) {
      indent(indentation_level + 1);
      printf("- %u\n", *leaf_node_key(node, i));
    }
    break;

  case NODE_INTERNAL:
    num_keys = *internal_node_num_keys(node);
    indent(indentation_level);
    printf("- internal (size %u)\n", num_keys);
    if (num_keys > 0) {
      for (uint32_t i = 0; i < num_keys; i++) {
        child = *internal_node_child(node, i);
        print_tree(pager, child, indentation_level + 1);
        indent(indentation_level + 1);
        printf("- key %u\n", *internal_node_key(node, i));
      }
      child = *internal_node_right_child(node);
      print_tree(pager, child, indentation_level + 1);
    }
    break;
  }
}

/**
 * 印出一筆 Row 資料
 */
void print_row(Row *row) {
  printf("(%u, %s, %s)\n", row->id, row->username, row->email);
}

/**
 * 取得下一個未使用的頁面編號
 */
uint32_t get_unused_page_num(Pager *pager) { return pager->num_pages; }

/* ============================================================================
 * Pager 管理（檔案 I/O 與頁面快取）
 * ============================================================================
 */

/**
 * 開啟資料庫檔案，初始化 Pager
 *
 * @param filename 資料庫檔案路徑
 * @return 初始化完成的 Pager 指標
 */
Pager *pager_open(const char *filename) {
  int fd = open(filename, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);

  if (fd == -1) {
    printf("Error: Unable to open database file '%s': %s\n", filename, strerror(errno));
    exit(EXIT_FAILURE);
  }

  off_t file_length = lseek(fd, 0, SEEK_END);

  if (file_length == -1) {
    printf("Error: Unable to determine file size for '%s': %s\n", filename, strerror(errno));
    close(fd);
    exit(EXIT_FAILURE);
  }

  Pager *pager = malloc(sizeof(Pager));
  if (pager == NULL) {
    printf("Error: Memory allocation failed for pager\n");
    close(fd);
    exit(EXIT_FAILURE);
  }

  pager->file_descriptor = fd;
  pager->file_length = file_length;
  pager->num_pages = (file_length / PAGE_SIZE);

  if (file_length % PAGE_SIZE != 0) {
    printf("Error: Database file '%s' is corrupted (size %lld is not a multiple of page size %u)\n", 
           filename, (long long)file_length, PAGE_SIZE);
    free(pager);
    close(fd);
    exit(EXIT_FAILURE);
  }

  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
    pager->pages[i] = NULL;
  }

  return pager;
}

/**
 * 取得指定頁面的記憶體位址，若未載入則從檔案讀取
 *
 * @param pager Pager 指標
 * @param page_num 頁面編號
 * @return 頁面的記憶體位址
 */
void *get_page(Pager *pager, uint32_t page_num) {
  if (page_num > TABLE_MAX_PAGES) {
    printf("Error: Page number %u exceeds maximum allowed pages (%d)\n", 
           page_num, TABLE_MAX_PAGES);
    exit(EXIT_FAILURE);
  }

  if (pager->pages[page_num] == NULL) {
    // Cache miss：配置記憶體並從檔案載入
    void *page = malloc(PAGE_SIZE);
    if (page == NULL) {
      printf("Error: Memory allocation failed for page %u\n", page_num);
      exit(EXIT_FAILURE);
    }

    uint32_t num_pages = pager->file_length / PAGE_SIZE;

    // 檔案末端可能有部分頁面
    if (pager->file_length % PAGE_SIZE) {
      num_pages += 1;
    }

    if (page_num <= num_pages) {
      off_t offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
      if (offset == -1) {
        printf("Error: Failed to seek to page %u: %s\n", page_num, strerror(errno));
        free(page);
        exit(EXIT_FAILURE);
      }

      ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);
      if (bytes_read == -1) {
        printf("Error: Failed to read page %u from file: %s\n", page_num, strerror(errno));
        free(page);
        exit(EXIT_FAILURE);
      }
    }

    pager->pages[page_num] = page;

    if (page_num >= pager->num_pages) {
      pager->num_pages = page_num + 1;
    }
  }

  return pager->pages[page_num];
}

/**
 * 將指定頁面寫回檔案
 *
 * @param pager Pager 指標
 * @param page_num 頁面編號
 */
void pager_flush(Pager *pager, uint32_t page_num) {
  if (pager->pages[page_num] == NULL) {
    printf("Error: Attempted to flush null page %u\n", page_num);
    exit(EXIT_FAILURE);
  }

  off_t offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
  if (offset == -1) {
    printf("Error: Failed to seek to page %u for writing: %s\n", page_num, strerror(errno));
    exit(EXIT_FAILURE);
  }

  ssize_t bytes_written =
      write(pager->file_descriptor, pager->pages[page_num], PAGE_SIZE);
  if (bytes_written == -1) {
    printf("Error: Failed to write page %u to file: %s\n", page_num, strerror(errno));
    exit(EXIT_FAILURE);
  }

  if (bytes_written != PAGE_SIZE) {
    printf("Error: Incomplete write for page %u (wrote %zd bytes, expected %u)\n", 
           page_num, bytes_written, PAGE_SIZE);
    exit(EXIT_FAILURE);
  }
}

/**
 * 開啟資料庫，初始化 Table 結構
 *
 * @param filename 資料庫檔案路徑
 * @return 初始化完成的 Table 指標
 */
Table *db_open(const char *filename) {
  Pager *pager = pager_open(filename);

  Table *table = malloc(sizeof(Table));
  if (table == NULL) {
    printf("Error: Memory allocation failed for table\n");
    // 清理 pager 資源
    close(pager->file_descriptor);
    free(pager);
    exit(EXIT_FAILURE);
  }

  table->pager = pager;
  table->root_page_num = 0;
  
  // 初始化交易結構
  table->transaction = malloc(sizeof(Transaction));
  if (table->transaction == NULL) {
    printf("Error: Memory allocation failed for transaction\n");
    close(pager->file_descriptor);
    free(pager);
    free(table);
    exit(EXIT_FAILURE);
  }

  table->transaction->state = TXN_STATE_NONE;
  table->transaction->num_modified = 0;
  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
    table->transaction->shadow_pages[i] = NULL;
    table->transaction->modified_pages[i] = false;
  }

  // 初始化統計資訊
  table->statistics = malloc(sizeof(TableStatistics));
  if (table->statistics == NULL) {
    printf("Error: Memory allocation failed for statistics\n");
    close(pager->file_descriptor);
    free(pager);
    free(table->transaction);
    free(table);
    exit(EXIT_FAILURE);
  }
  
  statistics_reset(table->statistics);
  
  // 嘗試載入統計資訊，如果載入失敗則收集新的統計資訊
  if (!statistics_load(table)) {
    // 如果表不為空，收集統計資訊
    if (pager->num_pages > 0) {
      TableStatistics *stats = collect_table_statistics(table);
      if (stats != NULL) {
        memcpy(table->statistics, stats, sizeof(TableStatistics));
        free(stats);
        statistics_save(table);
      }
    }
  }

  if (pager->num_pages == 0) {
    // 新資料庫檔案：初始化第 0 頁為葉節點
    void *root_node = get_page(pager, 0);
    initialize_leaf_node(root_node);
    set_node_root(root_node, true);
  }

  return table;
}

/**
 * 關閉資料庫，將所有頁面寫回檔案並釋放資源
 *
 * @param table Table 指標
 */
void db_close(Table *table) {
  Pager *pager = table->pager;
  
  // 如果有活動的交易，強制提交
  if (table->transaction && table->transaction->state == TXN_STATE_ACTIVE) {
    printf("Warning: Active transaction will be committed.\n");
    transaction_commit(table);
  }

  for (uint32_t i = 0; i < pager->num_pages; i++) {
    if (pager->pages[i] == NULL) {
      continue;
    }
    pager_flush(pager, i);
    free(pager->pages[i]);
    pager->pages[i] = NULL;
  }

  int result = close(pager->file_descriptor);
  if (result == -1) {
    printf("Error: Failed to close database file: %s\n", strerror(errno));
    exit(EXIT_FAILURE);
  }

  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
    void *page = pager->pages[i];
    if (page) {
      free(page);
      pager->pages[i] = NULL;
    }
  }

  // 清理交易資源
  if (table->transaction) {
    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
      if (table->transaction->shadow_pages[i]) {
        free(table->transaction->shadow_pages[i]);
      }
    }
    free(table->transaction);
  }

  // 保存並清理統計資訊
  if (table->statistics) {
    statistics_save(table);
    free(table->statistics);
  }

  free(pager);
  free(table);
}

/* ============================================================================
 * 交易管理（Transaction Management）
 * ============================================================================
 */

/**
 * 檢查是否在交易中
 *
 * @param table Table 指標
 * @return 是否在交易中
 */
bool is_in_transaction(Table *table) {
  return table->transaction && table->transaction->state == TXN_STATE_ACTIVE;
}

/**
 * 開始一個新交易
 *
 * @param table Table 指標
 * @return Transaction 指標
 */
Transaction *transaction_begin(Table *table) {
  if (is_in_transaction(table)) {
    printf("Error: Transaction already in progress.\n");
    return NULL;
  }

  Transaction *txn = table->transaction;
  txn->state = TXN_STATE_ACTIVE;
  txn->num_modified = 0;
  
  // 清空影子頁面
  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
    if (txn->shadow_pages[i]) {
      free(txn->shadow_pages[i]);
      txn->shadow_pages[i] = NULL;
    }
    txn->modified_pages[i] = false;
  }

  return txn;
}

/**
 * 取得頁面用於讀取
 * 如果在交易中且有影子頁面，返回影子頁面；否則返回實際頁面
 *
 * @param table Table 指標
 * @param page_num 頁面編號
 * @return 頁面指標
 */
void *get_page_for_read(Table *table, uint32_t page_num) {
  if (is_in_transaction(table) &&
      table->transaction->shadow_pages[page_num]) {
    return table->transaction->shadow_pages[page_num];
  }
  return get_page(table->pager, page_num);
}

/**
 * 取得用於寫入的頁面
 * 如果在交易中，返回影子頁面；否則返回實際頁面
 *
 * @param table Table 指標
 * @param page_num 頁面編號
 * @return 頁面指標
 */
void *get_page_for_write(Table *table, uint32_t page_num) {
  if (!is_in_transaction(table)) {
    // 不在交易中，直接返回實際頁面
    return get_page(table->pager, page_num);
  }

  Transaction *txn = table->transaction;
  
  // 如果這個頁面還沒有影子頁面，創建一個
  if (!txn->shadow_pages[page_num]) {
    // 創建影子頁面並複製原始頁面的內容
    txn->shadow_pages[page_num] = malloc(PAGE_SIZE);
    if (txn->shadow_pages[page_num] == NULL) {
      printf("Error: Memory allocation failed for shadow page %u\n", page_num);
      exit(EXIT_FAILURE);
    }

    void *original_page = get_page(table->pager, page_num);
    memcpy(txn->shadow_pages[page_num], original_page, PAGE_SIZE);
    
    // 標記這個頁面已被修改
    if (!txn->modified_pages[page_num]) {
      txn->modified_pages[page_num] = true;
      txn->num_modified++;
    }
  }

  return txn->shadow_pages[page_num];
}

/**
 * 提交交易
 * 將所有影子頁面的內容寫回實際頁面並持久化
 *
 * @param table Table 指標
 * @return 執行結果
 */
ExecuteResult transaction_commit(Table *table) {
  if (!is_in_transaction(table)) {
    printf("Error: No active transaction.\n");
    return EXECUTE_TABLE_FULL; // 借用這個錯誤碼
  }

  Transaction *txn = table->transaction;
  
  // 將所有影子頁面寫回實際頁面
  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
    if (txn->modified_pages[i] && txn->shadow_pages[i]) {
      void *original_page = get_page(table->pager, i);
      memcpy(original_page, txn->shadow_pages[i], PAGE_SIZE);
      
      // 立即寫回磁碟以確保持久性（Durability）
      pager_flush(table->pager, i);
      
      // 釋放影子頁面
      free(txn->shadow_pages[i]);
      txn->shadow_pages[i] = NULL;
      txn->modified_pages[i] = false;
    }
  }

  txn->state = TXN_STATE_COMMITTED;
  txn->num_modified = 0;
  
  return EXECUTE_SUCCESS;
}

/**
 * 回滾交易
 * 丟棄所有影子頁面，恢復到交易開始前的狀態
 *
 * @param table Table 指標
 * @return 執行結果
 */
ExecuteResult transaction_rollback(Table *table) {
  if (!is_in_transaction(table)) {
    printf("Error: No active transaction.\n");
    return EXECUTE_TABLE_FULL; // 借用這個錯誤碼
  }

  Transaction *txn = table->transaction;
  
  // 釋放所有影子頁面（丟棄所有修改）
  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
    if (txn->shadow_pages[i]) {
      free(txn->shadow_pages[i]);
      txn->shadow_pages[i] = NULL;
    }
    txn->modified_pages[i] = false;
  }

  txn->state = TXN_STATE_ABORTED;
  txn->num_modified = 0;
  
  return EXECUTE_SUCCESS;
}

/* ============================================================================
 * 節點操作（通用）
 * ============================================================================
 */

/**
 * 取得節點類型
 */
NodeType get_node_type(void *node) {
  uint8_t value = *((uint8_t *)(node + NODE_TYPE_OFFSET));
  return (NodeType)value;
}

/**
 * 設定節點類型
 */
void set_node_type(void *node, NodeType type) {
  uint8_t value = type;
  *((uint8_t *)(node + NODE_TYPE_OFFSET)) = value;
}

/**
 * 檢查節點是否為根節點
 */
bool is_node_root(void *node) {
  uint8_t value = *((uint8_t *)(node + IS_ROOT_OFFSET));
  return (bool)value;
}

/**
 * 設定節點的根節點旗標
 */
void set_node_root(void *node, bool is_root) {
  uint8_t value = is_root;
  *((uint8_t *)(node + IS_ROOT_OFFSET)) = value;
}

/**
 * 取得節點的父節點頁面編號指標
 */
uint32_t *node_parent(void *node) { return node + PARENT_POINTER_OFFSET; }

/**
 * 取得節點的最大鍵值（遞迴）
 *
 * @param pager Pager 指標
 * @param node 節點指標
 * @return 最大鍵值
 */
uint32_t get_node_max_key(Pager *pager, void *node) {
  if (get_node_type(node) == NODE_LEAF) {
    return *leaf_node_key(node, *leaf_node_num_cells(node) - 1);
  }
  void *right_child = get_page(pager, *internal_node_right_child(node));
  return get_node_max_key(pager, right_child);
}

/* ============================================================================
 * 葉節點操作
 * ============================================================================
 */

/**
 * 取得葉節點的 cell 數量指標
 */
uint32_t *leaf_node_num_cells(void *node) {
  return node + LEAF_NODE_NUM_CELLS_OFFSET;
}

/**
 * 取得葉節點的下一個葉節點頁面編號指標
 */
uint32_t *leaf_node_next_leaf(void *node) {
  return node + LEAF_NODE_NEXT_LEAF_OFFSET;
}

/**
 * 取得葉節點第 cell_num 個 cell 的位址
 */
void *leaf_node_cell(void *node, uint32_t cell_num) {
  return node + LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE;
}

/**
 * 取得葉節點第 cell_num 個 cell 的 key 指標
 */
uint32_t *leaf_node_key(void *node, uint32_t cell_num) {
  return leaf_node_cell(node, cell_num);
}

/**
 * 取得葉節點第 cell_num 個 cell 的 value 位址
 */
void *leaf_node_value(void *node, uint32_t cell_num) {
  return leaf_node_cell(node, cell_num) + LEAF_NODE_VALUE_OFFSET;
}

/**
 * 初始化葉節點
 */
void initialize_leaf_node(void *node) {
  set_node_type(node, NODE_LEAF);
  set_node_root(node, false);
  *leaf_node_num_cells(node) = 0;
  *leaf_node_next_leaf(node) = 0; // 0 表示沒有下一個葉節點
}

/**
 * 在葉節點中使用二分搜尋找到 key 的位置
 *
 * @param table Table 指標
 * @param page_num 頁面編號
 * @param key 要搜尋的鍵
 * @return Cursor 指標，指向 key 的位置或應插入的位置
 */
Cursor *leaf_node_find(Table *table, uint32_t page_num, uint32_t key) {
  void *node = get_page_for_read(table, page_num);
  uint32_t num_cells = *leaf_node_num_cells(node);

  Cursor *cursor = malloc(sizeof(Cursor));
  if (cursor == NULL) {
    printf("Error: Memory allocation failed for cursor\n");
    exit(EXIT_FAILURE);
  }

  cursor->table = table;
  cursor->page_num = page_num;
  cursor->end_of_table = false;  // 初始化 end_of_table

  // 二分搜尋
  uint32_t min_index = 0;
  uint32_t one_past_max_index = num_cells;
  while (one_past_max_index != min_index) {
    uint32_t index = (min_index + one_past_max_index) / 2;
    uint32_t key_at_index = *leaf_node_key(node, index);
    if (key == key_at_index) {
      cursor->cell_num = index;
      return cursor;
    }
    if (key < key_at_index) {
      one_past_max_index = index;
    } else {
      min_index = index + 1;
    }
  }

  cursor->cell_num = min_index;
  
  // 如果 cursor 指向超出範圍的位置，標記為 end_of_table
  if (cursor->cell_num >= num_cells) {
    cursor->end_of_table = true;
  }
  
  return cursor;
}

/**
 * 在葉節點中插入一筆資料
 *
 * @param cursor Cursor 指標
 * @param key 鍵
 * @param value Row 資料指標
 */
void leaf_node_insert(Cursor *cursor, uint32_t key, Row *value) {
  void *node = get_page_for_write(cursor->table, cursor->page_num);
  uint32_t num_cells = *leaf_node_num_cells(node);

  if (num_cells >= LEAF_NODE_MAX_CELLS) {
    // 節點已滿，需要分裂
    leaf_node_split_and_insert(cursor, key, value);
    return;
  }

  if (cursor->cell_num < num_cells) {
    // 為新 cell 騰出空間
    for (uint32_t i = num_cells; i > cursor->cell_num; i--) {
      memcpy(leaf_node_cell(node, i), leaf_node_cell(node, i - 1),
             LEAF_NODE_CELL_SIZE);
    }
  }

  *(leaf_node_num_cells(node)) = num_cells + 1;
  *(leaf_node_key(node, cursor->cell_num)) = key;
  serialize_row(value, leaf_node_value(node, cursor->cell_num));
}

/**
 * 從葉節點中刪除一筆資料
 *
 * @param cursor Cursor 指標，指向要刪除的 cell 位置
 */
void leaf_node_delete(Cursor *cursor) {
  void *node = get_page_for_write(cursor->table, cursor->page_num);
  uint32_t num_cells = *leaf_node_num_cells(node);

  if (cursor->cell_num >= num_cells) {
    // 超出範圍，不應該發生
    return;
  }

  // 將後面的 cell 向前移動以填補刪除的空缺
  for (uint32_t i = cursor->cell_num; i < num_cells - 1; i++) {
    memcpy(leaf_node_cell(node, i), leaf_node_cell(node, i + 1),
           LEAF_NODE_CELL_SIZE);
  }

  // 減少 cell 數量
  *(leaf_node_num_cells(node)) = num_cells - 1;

  // 檢查是否需要合併節點（只在節點為空且不是根節點時合併）
  uint32_t current_cells = *leaf_node_num_cells(node);
  if (current_cells == 0 && !is_node_root(node)) {
    // 找到父節點
    uint32_t parent_page_num = *node_parent(node);
    void *parent = get_page(cursor->table->pager, parent_page_num);

    // 找到當前節點在父節點中的位置
    uint32_t child_index = 0;
    uint32_t num_keys = *internal_node_num_keys(parent);

    for (uint32_t i = 0; i < num_keys; i++) {
      if (*internal_node_child(parent, i) == cursor->page_num) {
        child_index = i;
        break;
      }
    }

    // 只嘗試與左兄弟節點合併，避免複雜的邏輯
    if (child_index > 0) {
      uint32_t left_sibling_page =
          *internal_node_child(parent, child_index - 1);
      void *left_sibling = get_page(cursor->table->pager, left_sibling_page);
      uint32_t left_cells = *leaf_node_num_cells(left_sibling);

      // 如果左兄弟節點有空間，就合併
      if (left_cells < LEAF_NODE_MAX_CELLS) {
        leaf_node_merge(cursor->table, left_sibling_page, cursor->page_num);
        return;
      }
    }
  }
}

/**
 * 檢查葉節點是否應該與兄弟節點合併
 *
 * @param table Table 指標
 * @param page_num 頁面編號
 * @return 是否應該合併
 */
bool should_merge_leaf_nodes(Table *table, uint32_t page_num) {
  void *node = get_page(table->pager, page_num);
  uint32_t num_cells = *leaf_node_num_cells(node);

  // 如果節點為空，應該合併
  if (num_cells == 0) {
    return true;
  }

  // 如果節點不是根節點且 cell 數量少於閾值，考慮合併
  if (!is_node_root(node)) {
    uint32_t parent_page_num = *node_parent(node);
    void *parent = get_page(table->pager, parent_page_num);

    // 找到當前節點在父節點中的位置
    uint32_t child_index = 0;
    uint32_t num_keys = *internal_node_num_keys(parent);

    for (uint32_t i = 0; i < num_keys; i++) {
      if (*internal_node_child(parent, i) == page_num) {
        child_index = i;
        break;
      }
    }

    // 檢查左兄弟節點
    if (child_index > 0) {
      uint32_t left_sibling_page =
          *internal_node_child(parent, child_index - 1);
      void *left_sibling = get_page(table->pager, left_sibling_page);
      uint32_t left_cells = *leaf_node_num_cells(left_sibling);

      // 如果兩個節點的 cell 總數不超過最大容量，可以合併
      if (num_cells + left_cells <= LEAF_NODE_MAX_CELLS) {
        return true;
      }
    }

    // 檢查右兄弟節點
    if (child_index < num_keys) {
      uint32_t right_sibling_page =
          *internal_node_child(parent, child_index + 1);
      void *right_sibling = get_page(table->pager, right_sibling_page);
      uint32_t right_cells = *leaf_node_num_cells(right_sibling);

      // 如果兩個節點的 cell 總數不超過最大容量，可以合併
      if (num_cells + right_cells <= LEAF_NODE_MAX_CELLS) {
        return true;
      }
    }
  }

  return false;
}

/**
 * 合併兩個相鄰的葉節點
 *
 * @param table Table 指標
 * @param left_page_num 左節點頁面編號
 * @param right_page_num 右節點頁面編號
 */
void leaf_node_merge(Table *table, uint32_t left_page_num,
                     uint32_t right_page_num) {
  void *left_node = get_page(table->pager, left_page_num);
  void *right_node = get_page(table->pager, right_page_num);

  uint32_t left_cells = *leaf_node_num_cells(left_node);
  uint32_t right_cells = *leaf_node_num_cells(right_node);

  // 將右節點的所有 cell 移到左節點
  for (uint32_t i = 0; i < right_cells; i++) {
    void *source_cell = leaf_node_cell(right_node, i);
    void *dest_cell = leaf_node_cell(left_node, left_cells + i);
    memcpy(dest_cell, source_cell, LEAF_NODE_CELL_SIZE);
  }

  // 更新左節點的 cell 數量
  *(leaf_node_num_cells(left_node)) = left_cells + right_cells;

  // 更新左節點的 next_leaf 指標
  uint32_t right_next_leaf = *leaf_node_next_leaf(right_node);
  *(leaf_node_next_leaf(left_node)) = right_next_leaf;

  // 從父節點中移除右節點的引用
  uint32_t parent_page_num = *node_parent(right_node);
  void *parent = get_page(table->pager, parent_page_num);

  // 找到右節點在父節點中的位置
  uint32_t child_index = 0;
  uint32_t num_keys = *internal_node_num_keys(parent);

  for (uint32_t i = 0; i < num_keys; i++) {
    if (*internal_node_child(parent, i) == right_page_num) {
      child_index = i;
      break;
    }
  }

  // 移除右節點的引用
  for (uint32_t i = child_index; i < num_keys - 1; i++) {
    *internal_node_child(parent, i) = *internal_node_child(parent, i + 1);
    *internal_node_key(parent, i) = *internal_node_key(parent, i + 1);
  }

  // 減少父節點的鍵數量
  *(internal_node_num_keys(parent)) = num_keys - 1;

  // 釋放右節點的頁面
  table->pager->pages[right_page_num] = NULL;
}

/**
 * 檢查內部節點是否應該與兄弟節點合併
 *
 * @param table Table 指標
 * @param page_num 頁面編號
 * @return 是否應該合併
 */
bool should_merge_internal_nodes(Table *table, uint32_t page_num) {
  void *node = get_page(table->pager, page_num);
  uint32_t num_keys = *internal_node_num_keys(node);

  // 如果節點為空，應該合併
  if (num_keys == 0) {
    return true;
  }

  // 如果節點不是根節點且鍵數量少於閾值，考慮合併
  if (!is_node_root(node)) {
    uint32_t parent_page_num = *node_parent(node);
    void *parent = get_page(table->pager, parent_page_num);

    // 找到當前節點在父節點中的位置
    uint32_t child_index = 0;
    uint32_t parent_num_keys = *internal_node_num_keys(parent);

    for (uint32_t i = 0; i < parent_num_keys; i++) {
      if (*internal_node_child(parent, i) == page_num) {
        child_index = i;
        break;
      }
    }

    // 檢查左兄弟節點
    if (child_index > 0) {
      uint32_t left_sibling_page =
          *internal_node_child(parent, child_index - 1);
      void *left_sibling = get_page(table->pager, left_sibling_page);
      uint32_t left_keys = *internal_node_num_keys(left_sibling);

      // 如果兩個節點的鍵總數不超過最大容量，可以合併
      if (num_keys + left_keys + 1 <= INTERNAL_NODE_MAX_CELLS) {
        return true;
      }
    }

    // 檢查右兄弟節點
    if (child_index < parent_num_keys) {
      uint32_t right_sibling_page =
          *internal_node_child(parent, child_index + 1);
      void *right_sibling = get_page(table->pager, right_sibling_page);
      uint32_t right_keys = *internal_node_num_keys(right_sibling);

      // 如果兩個節點的鍵總數不超過最大容量，可以合併
      if (num_keys + right_keys + 1 <= INTERNAL_NODE_MAX_CELLS) {
        return true;
      }
    }
  }

  return false;
}

/**
 * 合併兩個相鄰的內部節點
 *
 * @param table Table 指標
 * @param parent_page_num 父節點頁面編號
 * @param left_page_num 左節點頁面編號
 * @param right_page_num 右節點頁面編號
 */
void internal_node_merge(Table *table, uint32_t parent_page_num,
                         uint32_t left_page_num, uint32_t right_page_num) {
  void *left_node = get_page(table->pager, left_page_num);
  void *right_node = get_page(table->pager, right_page_num);
  void *parent = get_page(table->pager, parent_page_num);

  uint32_t left_keys = *internal_node_num_keys(left_node);
  uint32_t right_keys = *internal_node_num_keys(right_node);

  // 從父節點中獲取分隔鍵
  uint32_t separator_key = 0;
  uint32_t num_keys = *internal_node_num_keys(parent);

  for (uint32_t i = 0; i < num_keys; i++) {
    if (*internal_node_child(parent, i) == left_page_num) {
      separator_key = *internal_node_key(parent, i);
      break;
    }
  }

  // 將分隔鍵添加到左節點
  *internal_node_key(left_node, left_keys) = separator_key;
  *internal_node_child(left_node, left_keys + 1) =
      *internal_node_child(right_node, 0);

  // 將右節點的所有鍵和子節點移到左節點
  for (uint32_t i = 0; i < right_keys; i++) {
    *internal_node_key(left_node, left_keys + 1 + i) =
        *internal_node_key(right_node, i);
    *internal_node_child(left_node, left_keys + 2 + i) =
        *internal_node_child(right_node, i + 1);
  }

  // 更新左節點的鍵數量
  *(internal_node_num_keys(left_node)) = left_keys + right_keys + 1;

  // 從父節點中移除右節點的引用
  uint32_t child_index = 0;
  for (uint32_t i = 0; i < num_keys; i++) {
    if (*internal_node_child(parent, i) == right_page_num) {
      child_index = i;
      break;
    }
  }

  // 移除右節點的引用
  for (uint32_t i = child_index; i < num_keys - 1; i++) {
    *internal_node_child(parent, i) = *internal_node_child(parent, i + 1);
    *internal_node_key(parent, i) = *internal_node_key(parent, i + 1);
  }

  // 減少父節點的鍵數量
  *(internal_node_num_keys(parent)) = num_keys - 1;

  // 釋放右節點的頁面
  table->pager->pages[right_page_num] = NULL;
}

/**
 * 葉節點分裂並插入新資料
 *
 * 當葉節點已滿時，將其分裂為左右兩個節點，並插入新資料。
 * 如果是根節點分裂，會創建新的根節點。
 *
 * @param cursor Cursor 指標
 * @param key 鍵
 * @param value Row 資料指標
 */
void leaf_node_split_and_insert(Cursor *cursor, uint32_t key, Row *value) {
  void *old_node = get_page(cursor->table->pager, cursor->page_num);
  uint32_t old_max = get_node_max_key(cursor->table->pager, old_node);
  uint32_t new_page_num = get_unused_page_num(cursor->table->pager);
  void *new_node = get_page(cursor->table->pager, new_page_num);
  initialize_leaf_node(new_node);
  *node_parent(new_node) = *node_parent(old_node);
  *leaf_node_next_leaf(new_node) = *leaf_node_next_leaf(old_node);
  *leaf_node_next_leaf(old_node) = new_page_num;

  /*
   * 將所有現有的 cell 加上新 cell 平均分配到左右兩個節點。
   * 從右側開始，將每個 cell 移動到正確的位置。
   */
  int32_t left_split = (int32_t)LEAF_NODE_LEFT_SPLIT_COUNT;
  int32_t insert_at = (int32_t)cursor->cell_num;
  for (int32_t i = LEAF_NODE_MAX_CELLS; i >= 0; i--) {
    void *destination_node;
    if (i >= left_split) {
      destination_node = new_node;
    } else {
      destination_node = old_node;
    }
    uint32_t index_within_node = (uint32_t)(i % left_split);
    void *destination = leaf_node_cell(destination_node, index_within_node);

    if (i == insert_at) {
      serialize_row(value,
                    leaf_node_value(destination_node, index_within_node));
      *leaf_node_key(destination_node, index_within_node) = key;
    } else if (i > insert_at) {
      memcpy(destination, leaf_node_cell(old_node, i - 1), LEAF_NODE_CELL_SIZE);
    } else {
      memcpy(destination, leaf_node_cell(old_node, i), LEAF_NODE_CELL_SIZE);
    }
  }

  // 更新葉節點的 cell 數量
  *(leaf_node_num_cells(old_node)) = LEAF_NODE_LEFT_SPLIT_COUNT;
  *(leaf_node_num_cells(new_node)) = LEAF_NODE_RIGHT_SPLIT_COUNT;

  if (is_node_root(old_node)) {
    return create_new_root(cursor->table, new_page_num);
  } else {
    uint32_t parent_page_num = *node_parent(old_node);
    uint32_t new_max = get_node_max_key(cursor->table->pager, old_node);
    void *parent = get_page(cursor->table->pager, parent_page_num);

    update_internal_node_key(parent, old_max, new_max);
    internal_node_insert(cursor->table, parent_page_num, new_page_num);
    return;
  }
}

/* ============================================================================
 * 內部節點操作
 * ============================================================================
 */

/**
 * 取得內部節點的 key 數量指標
 */
uint32_t *internal_node_num_keys(void *node) {
  return node + INTERNAL_NODE_NUM_KEYS_OFFSET;
}

/**
 * 取得內部節點最右側子節點的頁面編號指標
 */
uint32_t *internal_node_right_child(void *node) {
  return node + INTERNAL_NODE_RIGHT_CHILD_OFFSET;
}

/**
 * 取得內部節點第 cell_num 個 cell 的位址（僅用於內部計算）
 */
static inline uint32_t *internal_node_child_cell(void *node,
                                                 uint32_t cell_num) {
  return node + INTERNAL_NODE_HEADER_SIZE + cell_num * INTERNAL_NODE_CELL_SIZE;
}

/**
 * 取得內部節點第 cell_num 個 cell 的位址
 */
void *internal_node_cell(void *node, uint32_t cell_num) {
  return (void *)(node + INTERNAL_NODE_HEADER_SIZE +
                  cell_num * INTERNAL_NODE_CELL_SIZE);
}

/**
 * 取得內部節點第 child_num 個子節點的頁面編號指標
 *
 * 若 child_num == num_keys，返回 right_child。
 * 包含邊界檢查以防止越界存取。
 */
uint32_t *internal_node_child(void *node, uint32_t child_num) {
  uint32_t num_keys = *internal_node_num_keys(node);
  if (child_num > num_keys) {
    printf("Tried to access child_num %u > num_keys %u\n", child_num, num_keys);
    exit(EXIT_FAILURE);
  } else if (child_num == num_keys) {
    uint32_t *right_child = internal_node_right_child(node);
    if (*right_child == INVALID_PAGE_NUM) {
      printf("Tried to access right child of node, but was invalid page\n");
      exit(EXIT_FAILURE);
    }
    return right_child;
  } else {
    uint32_t *child = internal_node_child_cell(node, child_num);
    if (*child == INVALID_PAGE_NUM) {
      printf("Tried to access child %u of node, but was invalid page\n",
             child_num);
      exit(EXIT_FAILURE);
    }
    return child;
  }
}

/**
 * 取得內部節點第 key_num 個 key 的指標
 */
uint32_t *internal_node_key(void *node, uint32_t key_num) {
  return (void *)internal_node_cell(node, key_num) + INTERNAL_NODE_CHILD_SIZE;
}

/**
 * 初始化內部節點
 */
void initialize_internal_node(void *node) {
  set_node_type(node, NODE_INTERNAL);
  set_node_root(node, false);
  *internal_node_num_keys(node) = 0;
  /*
   * 必須將 right_child 初始化為 INVALID_PAGE_NUM。
   * 因為根節點的頁面編號是 0，若不初始化，可能會誤將 0 當作有效的 right_child。
   */
  *internal_node_right_child(node) = INVALID_PAGE_NUM;
}

/**
 * 在內部節點中使用二分搜尋找到應包含 key 的子節點索引
 *
 * @param node 內部節點指標
 * @param key 要搜尋的鍵
 * @return 子節點索引
 */
uint32_t internal_node_find_child(void *node, uint32_t key) {
  uint32_t num_keys = *internal_node_num_keys(node);
  uint32_t min_index = 0;
  uint32_t max_index = num_keys; // 子節點數量比 key 數量多 1

  while (max_index != min_index) {
    uint32_t index = (min_index + max_index) / 2;
    uint32_t key_to_right = *internal_node_key(node, index);
    if (key_to_right >= key) {
      max_index = index;
    } else {
      min_index = index + 1;
    }
  }

  return min_index;
}

/**
 * 在內部節點中搜尋 key（遞迴）
 *
 * @param table Table 指標
 * @param page_num 內部節點的頁面編號
 * @param key 要搜尋的鍵
 * @return Cursor 指標
 */
Cursor *internal_node_find(Table *table, uint32_t page_num, uint32_t key) {
  void *node = get_page(table->pager, page_num);
  uint32_t child_index = internal_node_find_child(node, key);
  uint32_t child_num = *internal_node_child(node, child_index);
  void *child = get_page(table->pager, child_num);

  switch (get_node_type(child)) {
  case NODE_LEAF:
    return leaf_node_find(table, child_num, key);
  case NODE_INTERNAL:
    return internal_node_find(table, child_num, key);
  }

  return NULL;
}

/**
 * 更新內部節點中的 key
 *
 * @param node 內部節點指標
 * @param old_key 舊的 key
 * @param new_key 新的 key
 */
void update_internal_node_key(void *node, uint32_t old_key, uint32_t new_key) {
  uint32_t old_child_index = internal_node_find_child(node, old_key);
  *internal_node_key(node, old_child_index) = new_key;
}

/**
 * 在內部節點中插入一個新的子節點
 *
 * 如果內部節點已滿，會觸發分裂。
 *
 * @param table Table 指標
 * @param parent_page_num 父節點（內部節點）的頁面編號
 * @param child_page_num 要插入的子節點頁面編號
 */
void internal_node_insert(Table *table, uint32_t parent_page_num,
                          uint32_t child_page_num) {
  void *parent = get_page(table->pager, parent_page_num);
  void *child = get_page(table->pager, child_page_num);
  uint32_t child_max_key = get_node_max_key(table->pager, child);
  uint32_t index = internal_node_find_child(parent, child_max_key);

  uint32_t original_num_keys = *internal_node_num_keys(parent);

  if (original_num_keys >= INTERNAL_NODE_MAX_CELLS) {
    internal_node_split_and_insert(table, parent_page_num, child_page_num);
    return;
  }

  uint32_t right_child_page_num = *internal_node_right_child(parent);

  // 若 right_child 為 INVALID_PAGE_NUM，表示此內部節點為空
  if (right_child_page_num == INVALID_PAGE_NUM) {
    *internal_node_right_child(parent) = child_page_num;
    return;
  }

  void *right_child = get_page(table->pager, right_child_page_num);

  /*
   * 若已達到節點的最大 cell 數量，不能在分裂前就增加計數。
   * 否則會創建一個未初始化的 key/child pair。
   */
  *internal_node_num_keys(parent) = original_num_keys + 1;

  if (child_max_key > get_node_max_key(table->pager, right_child)) {
    // 取代 right_child
    *internal_node_child(parent, original_num_keys) = right_child_page_num;
    *internal_node_key(parent, original_num_keys) =
        get_node_max_key(table->pager, right_child);
    *internal_node_right_child(parent) = child_page_num;
  } else {
    // 為新 cell 騰出空間
    for (uint32_t i = original_num_keys; i > index; i--) {
      void *destination = internal_node_cell(parent, i);
      void *source = internal_node_cell(parent, i - 1);
      memcpy(destination, source, INTERNAL_NODE_CELL_SIZE);
    }
    *internal_node_child(parent, index) = child_page_num;
    *internal_node_key(parent, index) = child_max_key;
  }
}

/**
 * 內部節點分裂並插入新子節點
 *
 * 當內部節點已滿時，將其分裂為左右兩個內部節點。
 * 如果是根節點分裂，會創建新的根節點。
 *
 * @param table Table 指標
 * @param parent_page_num 要分裂的內部節點頁面編號
 * @param child_page_num 要插入的子節點頁面編號
 */
void internal_node_split_and_insert(Table *table, uint32_t parent_page_num,
                                    uint32_t child_page_num) {
  uint32_t old_page_num = parent_page_num;
  void *old_node = get_page(table->pager, parent_page_num);
  uint32_t old_max = get_node_max_key(table->pager, old_node);

  void *child = get_page(table->pager, child_page_num);
  uint32_t child_max = get_node_max_key(table->pager, child);

  uint32_t new_page_num = get_unused_page_num(table->pager);
  uint32_t splitting_root = is_node_root(old_node);

  void *parent;
  void *new_node;
  if (splitting_root) {
    create_new_root(table, new_page_num);
    parent = get_page(table->pager, table->root_page_num);
    old_page_num = *internal_node_child(parent, 0);
    old_node = get_page(table->pager, old_page_num);
  } else {
    parent = get_page(table->pager, *node_parent(old_node));
    new_node = get_page(table->pager, new_page_num);
    initialize_internal_node(new_node);
  }

  uint32_t *old_num_keys = internal_node_num_keys(old_node);

  // 將 old_node 的 right_child 移到 new_node
  uint32_t cur_page_num = *internal_node_right_child(old_node);
  void *cur = get_page(table->pager, cur_page_num);
  internal_node_insert(table, new_page_num, cur_page_num);
  *node_parent(cur) = new_page_num;
  *internal_node_right_child(old_node) = INVALID_PAGE_NUM;

  // 將右半部的 cell 移到 new_node
  for (int i = (int)INTERNAL_NODE_MAX_CELLS - 1; i > (int)(INTERNAL_NODE_MAX_CELLS / 2);
       i--) {
    cur_page_num = *internal_node_child(old_node, i);
    cur = get_page(table->pager, cur_page_num);
    internal_node_insert(table, new_page_num, cur_page_num);
    *node_parent(cur) = new_page_num;

    // 修正：正確地遞減 key 數量
    (*old_num_keys) = (*old_num_keys) - 1;
  }

  // 將最後一個 child 設為 old_node 的 right_child
  *internal_node_right_child(old_node) =
      *internal_node_child(old_node, (*old_num_keys) - 1);
  (*old_num_keys) = (*old_num_keys) - 1;

  uint32_t max_after_split = get_node_max_key(table->pager, old_node);

  // 決定新 child 應插入到哪個節點
  uint32_t destination_page_num =
      child_max < max_after_split ? old_page_num : new_page_num;
  internal_node_insert(table, destination_page_num, child_page_num);
  *node_parent(child) = destination_page_num;

  update_internal_node_key(parent, old_max,
                           get_node_max_key(table->pager, old_node));

  if (!splitting_root) {
    internal_node_insert(table, *node_parent(old_node), new_page_num);
    *node_parent(new_node) = *node_parent(old_node);
  }
}

/* ============================================================================
 * B-Tree 分裂與重組
 * ============================================================================
 */

/**
 * 創建新的根節點
 *
 * 當根節點需要分裂時，舊的根節點會被複製到新的頁面作為左子節點，
 * 然後根節點被重新初始化為內部節點，包含兩個子節點。
 *
 * @param table Table 指標
 * @param right_child_page_num 右子節點的頁面編號
 */
void create_new_root(Table *table, uint32_t right_child_page_num) {
  void *root = get_page(table->pager, table->root_page_num);
  void *right_child = get_page(table->pager, right_child_page_num);
  uint32_t left_child_page_num = get_unused_page_num(table->pager);
  void *left_child = get_page(table->pager, left_child_page_num);

  if (get_node_type(root) == NODE_INTERNAL) {
    initialize_internal_node(right_child);
    initialize_internal_node(left_child);
  }

  // 左子節點包含舊根節點的資料
  memcpy(left_child, root, PAGE_SIZE);
  set_node_root(left_child, false);

  // 若左子節點是內部節點，更新其所有子節點的 parent 指標
  if (get_node_type(left_child) == NODE_INTERNAL) {
    void *child;
    for (uint32_t i = 0; i < *internal_node_num_keys(left_child); i++) {
      child = get_page(table->pager, *internal_node_child(left_child, i));
      *node_parent(child) = left_child_page_num;
    }
    child = get_page(table->pager, *internal_node_right_child(left_child));
    *node_parent(child) = left_child_page_num;
  }

  // 根節點現在是一個內部節點，包含一個 key 和兩個子節點
  initialize_internal_node(root);
  set_node_root(root, true);
  *internal_node_num_keys(root) = 1;
  *internal_node_child(root, 0) = left_child_page_num;
  uint32_t left_child_max_key = get_node_max_key(table->pager, left_child);
  *internal_node_key(root, 0) = left_child_max_key;
  *internal_node_right_child(root) = right_child_page_num;
  *node_parent(left_child) = table->root_page_num;
  *node_parent(right_child) = table->root_page_num;
}

/* ============================================================================
 * Cursor 操作
 * ============================================================================
 */

/**
 * 在 B-tree 中搜尋 key 的位置
 *
 * @param table Table 指標
 * @param key 要搜尋的鍵
 * @return Cursor 指標
 */
Cursor *table_find(Table *table, uint32_t key) {
  uint32_t root_page_num = table->root_page_num;
  void *root_node = get_page(table->pager, root_page_num);

  if (get_node_type(root_node) == NODE_LEAF) {
    return leaf_node_find(table, root_page_num, key);
  } else {
    return internal_node_find(table, root_page_num, key);
  }
}

/**
 * 創建指向 B-tree 開頭的 cursor
 *
 * @param table Table 指標
 * @return Cursor 指標
 */
Cursor *table_start(Table *table) {
  Cursor *cursor = table_find(table, 0);
  void *node = get_page_for_read(table, cursor->page_num);
  uint32_t num_cells = *leaf_node_num_cells(node);
  cursor->end_of_table = (num_cells == 0);
  return cursor;
}

/**
 * 取得 cursor 指向的 Row 資料位址
 *
 * @param cursor Cursor 指標
 * @return Row 資料位址
 */
void *cursor_value(Cursor *cursor) {
  uint32_t page_num = cursor->page_num;
  void *page;
  
  // 如果在交易中且有影子頁面，從影子頁面讀取
  if (is_in_transaction(cursor->table) &&
      cursor->table->transaction->shadow_pages[page_num]) {
    page = cursor->table->transaction->shadow_pages[page_num];
  } else {
    page = get_page(cursor->table->pager, page_num);
  }
  
  return leaf_node_value(page, cursor->cell_num);
}

/**
 * 將 cursor 移動到下一筆資料
 *
 * @param cursor Cursor 指標
 */
void cursor_advance(Cursor *cursor) {
  uint32_t page_num = cursor->page_num;
  void *node = get_page_for_read(cursor->table, page_num);

  cursor->cell_num += 1;
  if (cursor->cell_num >= (*leaf_node_num_cells(node))) {
    // 移動到下一個葉節點
    uint32_t next_page_num = *leaf_node_next_leaf(node);
    if (next_page_num == 0) {
      // 這是最右側的葉節點
      cursor->end_of_table = true;
    } else {
      cursor->page_num = next_page_num;
      cursor->cell_num = 0;
    }
  }
}

/* ============================================================================
 * Row 序列化與反序列化
 * ============================================================================
 */

/**
 * 將 Row 結構序列化到記憶體
 *
 * @param source Row 資料指標
 * @param destination 目標記憶體位址
 */
void serialize_row(Row *source, void *destination) {
  memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
  strncpy(destination + USERNAME_OFFSET, source->username, USERNAME_SIZE);
  strncpy(destination + EMAIL_OFFSET, source->email, EMAIL_SIZE);
}

/**
 * 從記憶體反序列化 Row 結構
 *
 * @param source 來源記憶體位址
 * @param destination Row 資料指標
 */
void deserialize_row(void *source, Row *destination) {
  memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
  memcpy(&(destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
  memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}

/* ============================================================================
 * 輸入處理
 * ============================================================================
 */

/**
 * 創建新的輸入緩衝區
 *
 * @return InputBuffer 指標
 */
InputBuffer *new_input_buffer(void) {
  InputBuffer *input_buffer = malloc(sizeof(InputBuffer));
  if (input_buffer == NULL) {
    printf("Error: Memory allocation failed for input buffer\n");
    exit(EXIT_FAILURE);
  }

  input_buffer->buffer = NULL;
  input_buffer->buffer_length = 0;
  input_buffer->input_length = 0;
  return input_buffer;
}

/**
 * 釋放輸入緩衝區
 *
 * @param input_buffer InputBuffer 指標
 */
void close_input_buffer(InputBuffer *input_buffer) {
  free(input_buffer->buffer);
  free(input_buffer);
}

/**
 * 印出命令提示符號
 */
void print_prompt(void) { printf("db > "); }

/**
 * 讀取使用者輸入
 *
 * @param input_buffer InputBuffer 指標
 */
void read_input(InputBuffer *input_buffer) {
  ssize_t bytes_read =
      getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);

  if (bytes_read <= 0) {
    if (feof(stdin)) {
      printf("\nExiting...\n");
      exit(EXIT_SUCCESS);
    }
    printf("Error: Failed to read input: %s\n", strerror(errno));
    exit(EXIT_FAILURE);
  }

  // 移除尾端的換行符號
  input_buffer->input_length = bytes_read - 1;
  input_buffer->buffer[bytes_read - 1] = 0;
}

/* ============================================================================
 * 元命令處理（以 . 開頭的命令）
 * ============================================================================
 */

/**
 * 執行元命令
 *
 * @param input_buffer InputBuffer 指標
 * @param table Table 指標
 * @return 執行結果
 */
MetaCommandResult do_meta_command(InputBuffer *input_buffer, Table *table) {
  if (strcmp(input_buffer->buffer, ".exit") == 0) {
    close_input_buffer(input_buffer);
    db_close(table);
    exit(EXIT_SUCCESS);
  } else if (strcmp(input_buffer->buffer, ".btree") == 0) {
    printf("Tree:\n");
    print_tree(table->pager, 0, 0);
    return META_COMMAND_SUCCESS;
  } else if (strcmp(input_buffer->buffer, ".constants") == 0) {
    printf("Constants:\n");
    print_constants();
    return META_COMMAND_SUCCESS;
  } else if (strcmp(input_buffer->buffer, "begin") == 0 ||
             strcmp(input_buffer->buffer, "BEGIN") == 0 ||
             strcmp(input_buffer->buffer, "begin transaction") == 0 ||
             strcmp(input_buffer->buffer, "BEGIN TRANSACTION") == 0) {
    if (transaction_begin(table)) {
      printf("Transaction started.\n");
    }
    return META_COMMAND_SUCCESS;
  } else if (strcmp(input_buffer->buffer, "commit") == 0 ||
             strcmp(input_buffer->buffer, "COMMIT") == 0) {
    if (transaction_commit(table) == EXECUTE_SUCCESS) {
      printf("Transaction committed.\n");
    }
    return META_COMMAND_SUCCESS;
  } else if (strcmp(input_buffer->buffer, "rollback") == 0 ||
             strcmp(input_buffer->buffer, "ROLLBACK") == 0) {
    if (transaction_rollback(table) == EXECUTE_SUCCESS) {
      printf("Transaction rolled back.\n");
    }
    return META_COMMAND_SUCCESS;
  } else if (strcmp(input_buffer->buffer, ".analyze") == 0 ||
             strcmp(input_buffer->buffer, "analyze") == 0 ||
             strcmp(input_buffer->buffer, "ANALYZE") == 0) {
    // 收集並更新統計資訊
    printf("Analyzing table statistics...\n");
    TableStatistics *new_stats = collect_table_statistics(table);
    if (new_stats != NULL) {
      memcpy(table->statistics, new_stats, sizeof(TableStatistics));
      free(new_stats);
      statistics_save(table);
      printf("Statistics updated successfully.\n");
      printf("  Total rows: %u\n", table->statistics->total_rows);
      printf("  ID range: %u - %u\n", table->statistics->id_min, table->statistics->id_max);
      printf("  ID cardinality: %u\n", table->statistics->id_cardinality);
      printf("  Username cardinality: %u\n", table->statistics->username_cardinality);
      printf("  Email cardinality: %u\n", table->statistics->email_cardinality);
    } else {
      printf("Error: Failed to collect statistics.\n");
    }
    return META_COMMAND_SUCCESS;
  } else if (strcmp(input_buffer->buffer, ".stats") == 0) {
    // 顯示當前統計資訊
    if (table->statistics && table->statistics->is_valid) {
      printf("Table Statistics:\n");
      printf("  Total rows: %u\n", table->statistics->total_rows);
      printf("  ID range: %u - %u\n", table->statistics->id_min, table->statistics->id_max);
      printf("  ID cardinality: %u\n", table->statistics->id_cardinality);
      printf("  Username cardinality: %u\n", table->statistics->username_cardinality);
      printf("  Email cardinality: %u\n", table->statistics->email_cardinality);
    } else {
      printf("Statistics not available. Run ANALYZE to collect statistics.\n");
    }
    return META_COMMAND_SUCCESS;
  } else {
    return META_COMMAND_UNRECOGNIZED_COMMAND;
  }
}

/* ============================================================================
 * SQL 語句解析
 * ============================================================================
 */

/**
 * 解析 INSERT 語句
 *
 * @param input_buffer InputBuffer 指標
 * @param statement Statement 指標
 * @return 解析結果
 */
PrepareResult prepare_insert(InputBuffer *input_buffer, Statement *statement) {
  statement->type = STATEMENT_INSERT;

  (void)strtok(input_buffer->buffer, " "); // 跳過 "insert" 關鍵字
  char *id_string = strtok(NULL, " ");
  char *username = strtok(NULL, " ");
  char *email = strtok(NULL, " ");

  if (id_string == NULL) {
    printf("Error: INSERT statement missing ID\n");
    return PREPARE_SYNTAX_ERROR;
  }

  if (username == NULL) {
    printf("Error: INSERT statement missing username\n");
    return PREPARE_SYNTAX_ERROR;
  }

  if (email == NULL) {
    printf("Error: INSERT statement missing email\n");
    return PREPARE_SYNTAX_ERROR;
  }

  int id = atoi(id_string);
  if (id <= 0) {
    printf("Error: ID must be a positive integer (got '%s')\n", id_string);
    return PREPARE_NEGATIVE_ID;
  }

  if (strlen(username) > COLUMN_USERNAME_SIZE) {
    printf("Error: Username exceeds maximum length of %d characters (got %zu)\n", 
           COLUMN_USERNAME_SIZE, strlen(username));
    return PREPARE_STRING_TOO_LONG;
  }

  if (strlen(email) > COLUMN_EMAIL_SIZE) {
    printf("Error: Email exceeds maximum length of %d characters (got %zu)\n", 
           COLUMN_EMAIL_SIZE, strlen(email));
    return PREPARE_STRING_TOO_LONG;
  }

  statement->row_to_insert.id = id;
  strcpy(statement->row_to_insert.username, username);
  strcpy(statement->row_to_insert.email, email);

  return PREPARE_SUCCESS;
}

/**
 * 解析 UPDATE 語句
 *
 * 支援兩種語法：
 * 1. 舊式語法：update [id] [username] [email]
 * 2. WHERE 語法：update [username] [email] where [condition]
 *
 * 使用 '-' 表示不更新該欄位
 *
 * 範例：
 *   update 1 new_username new_email@example.com  (更新兩個欄位)
 *   update 1 - new_email@example.com             (只更新 email)
 *   update 1 new_username -                      (只更新 username)
 *   update new_user new@example.com where id = 5 (使用 WHERE 子句)
 *
 * @param input_buffer InputBuffer 指標
 * @param statement Statement 指標
 * @return 解析結果
 */
PrepareResult prepare_update(InputBuffer *input_buffer, Statement *statement) {
  statement->type = STATEMENT_UPDATE;
  statement->update_username = false;
  statement->update_email = false;
  statement->where.field = WHERE_FIELD_NONE;
  statement->where.num_conditions = 0; // 初始化條件數量
  statement->where.num_expr_nodes = 0;
  statement->where.root_expr = INVALID_EXPR_INDEX;
  statement->where.use_expr_tree = false;

  (void)strtok(input_buffer->buffer, " "); // 跳過 "update" 關鍵字
  char *first_arg = strtok(NULL, " ");
  char *second_arg = strtok(NULL, " ");
  char *third_arg = strtok(NULL, " ");

  if (first_arg == NULL || second_arg == NULL) {
    printf("Error: UPDATE statement requires at least username and email\n");
    return PREPARE_SYNTAX_ERROR;
  }

  // 檢查是否有 WHERE 子句（新式語法）
  if (third_arg != NULL && strcmp(third_arg, "where") == 0) {
    // 新式語法：update [username] [email] where [condition]
    char *username = first_arg;
    char *email = second_arg;
    char *remaining = strtok(NULL, "");

    if (remaining == NULL) {
      printf("Error: UPDATE statement with WHERE clause requires condition\n");
      return PREPARE_SYNTAX_ERROR;
    }

    // 檢查是否要更新 username（'-' 表示不更新）
    if (strcmp(username, "-") != 0) {
      if (strlen(username) > COLUMN_USERNAME_SIZE) {
        printf("Error: Username exceeds maximum length of %d characters (got %zu)\n", 
               COLUMN_USERNAME_SIZE, strlen(username));
        return PREPARE_STRING_TOO_LONG;
      }
      strcpy(statement->row_to_insert.username, username);
      statement->update_username = true;
    }

    // 檢查是否要更新 email（'-' 表示不更新）
    if (strcmp(email, "-") != 0) {
      if (strlen(email) > COLUMN_EMAIL_SIZE) {
        printf("Error: Email exceeds maximum length of %d characters (got %zu)\n", 
               COLUMN_EMAIL_SIZE, strlen(email));
        return PREPARE_STRING_TOO_LONG;
      }
      strcpy(statement->row_to_insert.email, email);
      statement->update_email = true;
    }

    return parse_where_clause(remaining, &statement->where);
  }

  // 舊式語法：update [id] [username] [email]
  char *id_string = first_arg;
  char *username = second_arg;
  char *email = third_arg;

  if (email == NULL) {
    printf("Error: UPDATE statement requires ID, username, and email\n");
    return PREPARE_SYNTAX_ERROR;
  }

  int id = atoi(id_string);
  if (id <= 0) {
    printf("Error: ID must be a positive integer (got '%s')\n", id_string);
    return PREPARE_NEGATIVE_ID;
  }

  // 檢查是否要更新 username（'-' 表示不更新）
  if (strcmp(username, "-") != 0) {
    if (strlen(username) > COLUMN_USERNAME_SIZE) {
      printf("Error: Username exceeds maximum length of %d characters (got %zu)\n", 
             COLUMN_USERNAME_SIZE, strlen(username));
      return PREPARE_STRING_TOO_LONG;
    }
    strcpy(statement->row_to_insert.username, username);
    statement->update_username = true;
  }

  // 檢查是否要更新 email（'-' 表示不更新）
  if (strcmp(email, "-") != 0) {
    if (strlen(email) > COLUMN_EMAIL_SIZE) {
      printf("Error: Email exceeds maximum length of %d characters (got %zu)\n", 
             COLUMN_EMAIL_SIZE, strlen(email));
      return PREPARE_STRING_TOO_LONG;
    }
    strcpy(statement->row_to_insert.email, email);
    statement->update_email = true;
  }

  statement->row_to_insert.id = id;
  // 設置 WHERE 條件為 id = [指定的 id]
  statement->where.field = WHERE_FIELD_ID;
  statement->where.op = WHERE_OP_EQUAL;
  statement->where.value.id_value = id;

  return PREPARE_SUCCESS;
}

/**
 * 解析 DELETE 語句
 *
 * @param input_buffer InputBuffer 指標
 * @param statement Statement 指標
 * @return 解析結果
 */
PrepareResult prepare_delete(InputBuffer *input_buffer, Statement *statement) {
  statement->type = STATEMENT_DELETE;
  statement->where.field = WHERE_FIELD_NONE;
  statement->where.num_conditions = 0; // 初始化條件數量
  statement->where.num_expr_nodes = 0;
  statement->where.root_expr = INVALID_EXPR_INDEX;
  statement->where.use_expr_tree = false;

  (void)strtok(input_buffer->buffer, " "); // 跳過 "delete" 關鍵字
  char *id_string = strtok(NULL, " ");

  // 檢查是否有 WHERE 子句
  if (id_string != NULL && strcmp(id_string, "where") == 0) {
    // 有 WHERE 子句
    char *remaining = strtok(NULL, "");
    if (remaining == NULL) {
      printf("Error: DELETE statement with WHERE clause requires condition\n");
      return PREPARE_SYNTAX_ERROR;
    }
    return parse_where_clause(remaining, &statement->where);
  }

  // 沒有 WHERE 子句，舊式語法：delete [id]
  if (id_string == NULL) {
    printf("Error: DELETE statement requires ID or WHERE clause\n");
    return PREPARE_SYNTAX_ERROR;
  }

  int id = atoi(id_string);
  if (id <= 0) {
    printf("Error: ID must be a positive integer (got '%s')\n", id_string);
    return PREPARE_NEGATIVE_ID;
  }

  statement->row_to_insert.id = id;
  // 設置 WHERE 條件為 id = [指定的 id]
  statement->where.field = WHERE_FIELD_ID;
  statement->where.op = WHERE_OP_EQUAL;
  statement->where.value.id_value = id;

  return PREPARE_SUCCESS;
}

/**
 * 跳過空白字元
 */
void skip_whitespace(char **input) {
  while (**input == ' ' || **input == '\t' || **input == '\n' || **input == '\r') {
    (*input)++;
  }
}

/**
 * 解析基本條件：field operator value
 *
 * @param input 輸入字串指標的指標
 * @param condition 基本條件指標
 * @return 解析結果
 */
PrepareResult parse_basic_condition(char **input, WhereBasicCondition *condition) {
  skip_whitespace(input);
  
  // 解析欄位名稱
  char field_name[32];
  int i = 0;
  while ((**input >= 'a' && **input <= 'z') || (**input >= 'A' && **input <= 'Z') || **input == '_') {
    if (i < 31) {
      field_name[i++] = **input;
    }
    (*input)++;
  }
  field_name[i] = '\0';
  
  if (i == 0) {
    printf("Error: WHERE clause missing field name\n");
    return PREPARE_SYNTAX_ERROR;
  }
  
  if (i >= 31) {
    printf("Error: Field name too long in WHERE clause\n");
    return PREPARE_SYNTAX_ERROR;
  }
  
  // 判斷欄位類型
  if (strcmp(field_name, "id") == 0) {
    condition->field = WHERE_FIELD_ID;
  } else if (strcmp(field_name, "username") == 0) {
    condition->field = WHERE_FIELD_USERNAME;
  } else if (strcmp(field_name, "email") == 0) {
    condition->field = WHERE_FIELD_EMAIL;
  } else {
    printf("Error: Unknown field '%s' in WHERE clause (valid fields: id, username, email)\n", field_name);
    return PREPARE_SYNTAX_ERROR;
  }
  
  skip_whitespace(input);
  
  // 解析運算符
  if (strncmp(*input, ">=", 2) == 0) {
    condition->op = WHERE_OP_GREATER_EQUAL;
    (*input) += 2;
  } else if (strncmp(*input, "<=", 2) == 0) {
    condition->op = WHERE_OP_LESS_EQUAL;
    (*input) += 2;
  } else if (strncmp(*input, "!=", 2) == 0) {
    condition->op = WHERE_OP_NOT_EQUAL;
    (*input) += 2;
  } else if (**input == '=') {
    condition->op = WHERE_OP_EQUAL;
    (*input)++;
  } else if (**input == '>') {
    condition->op = WHERE_OP_GREATER;
    (*input)++;
  } else if (**input == '<') {
    condition->op = WHERE_OP_LESS;
    (*input)++;
  } else {
    printf("Error: Invalid operator in WHERE clause (valid operators: =, !=, >, <, >=, <=)\n");
    return PREPARE_SYNTAX_ERROR;
  }
  
  skip_whitespace(input);
  
  // 解析值
  char value_string[256];
  i = 0;
  while (**input != '\0' && **input != ' ' && **input != ')' && **input != '\t' && **input != '\n') {
    if (i < 255) {
      value_string[i++] = **input;
    }
    (*input)++;
  }
  value_string[i] = '\0';
  
  if (i == 0) {
    printf("Error: WHERE clause missing value for condition\n");
    return PREPARE_SYNTAX_ERROR;
  }
  
  if (i >= 255) {
    printf("Error: Value too long in WHERE clause (maximum 255 characters)\n");
    return PREPARE_SYNTAX_ERROR;
  }
  
  // 根據欄位類型設定值
  if (condition->field == WHERE_FIELD_ID) {
    int id = atoi(value_string);
    if (id < 0) {
      return PREPARE_NEGATIVE_ID;
    }
    condition->value.id_value = (uint32_t)id;
  } else {
    if (strlen(value_string) > 255) {
      return PREPARE_STRING_TOO_LONG;
    }
    strncpy(condition->value.string_value, value_string, 255);
    condition->value.string_value[255] = '\0';
  }
  
  return PREPARE_SUCCESS;
}

/**
 * 解析主表達式：括號表達式或基本條件
 *
 * @param input 輸入字串指標的指標
 * @param where WhereCondition 指標
 * @param expr_idx 表達式索引指標
 * @return 解析結果
 */
PrepareResult parse_where_primary_expr(char **input, WhereCondition *where, uint32_t *expr_idx) {
  skip_whitespace(input);
  
  if (**input == '(') {
    // 括號表達式
    (*input)++;
    PrepareResult result = parse_where_expression(input, where, expr_idx);
    if (result != PREPARE_SUCCESS) {
      return result;
    }
    skip_whitespace(input);
    if (**input != ')') {
      printf("Error: Missing closing parenthesis in WHERE clause\n");
      return PREPARE_SYNTAX_ERROR;
    }
    (*input)++;
    return PREPARE_SUCCESS;
  } else {
    // 基本條件
    if (where->num_expr_nodes >= MAX_WHERE_EXPR_NODES) {
      printf("Error: WHERE clause too complex (maximum %d expression nodes)\n", MAX_WHERE_EXPR_NODES);
      return PREPARE_SYNTAX_ERROR;
    }
    
    uint32_t node_idx = where->num_expr_nodes++;
    WhereExprNode *node = &where->expr_nodes[node_idx];
    node->type = WHERE_EXPR_BASIC;
    
    PrepareResult result = parse_basic_condition(input, &node->data.basic);
    if (result != PREPARE_SUCCESS) {
      where->num_expr_nodes--;
      return result;
    }
    
    *expr_idx = node_idx;
    return PREPARE_SUCCESS;
  }
}

/**
 * 解析 AND 表達式
 *
 * @param input 輸入字串指標的指標
 * @param where WhereCondition 指標
 * @param expr_idx 表達式索引指標
 * @return 解析結果
 */
PrepareResult parse_where_and_expr(char **input, WhereCondition *where, uint32_t *expr_idx) {
  PrepareResult result = parse_where_primary_expr(input, where, expr_idx);
  if (result != PREPARE_SUCCESS) {
    return result;
  }
  
  while (true) {
    skip_whitespace(input);
    
    // 檢查是否有 AND
    if (strncmp(*input, "and", 3) == 0 || strncmp(*input, "AND", 3) == 0) {
      char next_char = (*input)[3];
      if (next_char == ' ' || next_char == '\t' || next_char == '\0' || next_char == ')') {
        (*input) += 3;
        
        // 解析右側表達式
        uint32_t right_idx;
        result = parse_where_primary_expr(input, where, &right_idx);
        if (result != PREPARE_SUCCESS) {
          return result;
        }
        
        // 創建 AND 節點
        if (where->num_expr_nodes >= MAX_WHERE_EXPR_NODES) {
          return PREPARE_SYNTAX_ERROR;
        }
        
        uint32_t and_idx = where->num_expr_nodes++;
        WhereExprNode *and_node = &where->expr_nodes[and_idx];
        and_node->type = WHERE_EXPR_AND;
        and_node->data.logical.left = *expr_idx;
        and_node->data.logical.right = right_idx;
        
        *expr_idx = and_idx;
      } else {
        break;
      }
    } else {
      break;
    }
  }
  
  return PREPARE_SUCCESS;
}

/**
 * 解析 OR 表達式
 *
 * @param input 輸入字串指標的指標
 * @param where WhereCondition 指標
 * @param expr_idx 表達式索引指標
 * @return 解析結果
 */
PrepareResult parse_where_or_expr(char **input, WhereCondition *where, uint32_t *expr_idx) {
  PrepareResult result = parse_where_and_expr(input, where, expr_idx);
  if (result != PREPARE_SUCCESS) {
    return result;
  }
  
  while (true) {
    skip_whitespace(input);
    
    // 檢查是否有 OR
    if (strncmp(*input, "or", 2) == 0 || strncmp(*input, "OR", 2) == 0) {
      char next_char = (*input)[2];
      if (next_char == ' ' || next_char == '\t' || next_char == '\0' || next_char == ')') {
        (*input) += 2;
        
        // 解析右側表達式
        uint32_t right_idx;
        result = parse_where_and_expr(input, where, &right_idx);
        if (result != PREPARE_SUCCESS) {
          return result;
        }
        
        // 創建 OR 節點
        if (where->num_expr_nodes >= MAX_WHERE_EXPR_NODES) {
          return PREPARE_SYNTAX_ERROR;
        }
        
        uint32_t or_idx = where->num_expr_nodes++;
        WhereExprNode *or_node = &where->expr_nodes[or_idx];
        or_node->type = WHERE_EXPR_OR;
        or_node->data.logical.left = *expr_idx;
        or_node->data.logical.right = right_idx;
        
        *expr_idx = or_idx;
      } else {
        break;
      }
    } else {
      break;
    }
  }
  
  return PREPARE_SUCCESS;
}

/**
 * 解析 WHERE 表達式（頂層）
 *
 * @param input 輸入字串指標的指標
 * @param where WhereCondition 指標
 * @param expr_idx 表達式索引指標
 * @return 解析結果
 */
PrepareResult parse_where_expression(char **input, WhereCondition *where, uint32_t *expr_idx) {
  return parse_where_or_expr(input, where, expr_idx);
}

/**
 * 解析 WHERE 子句
 *
 * 支援的語法：
 *   - 單一條件：field op value
 *   - 複雜條件：field op value AND/OR field op value ...
 *   - 括號表達式：(field op value AND field op value) OR field op value
 * 例如：id = 5, username = john AND id > 10, (id < 100 OR id > 200) AND username = admin
 *
 * @param where_clause WHERE 子句字串
 * @param where WhereCondition 指標
 * @return 解析結果
 */
PrepareResult parse_where_clause(char *where_clause, WhereCondition *where) {
  // 複製字串以免破壞原始資料
  char clause_copy[1024];
  strncpy(clause_copy, where_clause, sizeof(clause_copy) - 1);
  clause_copy[sizeof(clause_copy) - 1] = '\0';

  // 初始化 WHERE 條件
  where->num_conditions = 0;
  where->num_expr_nodes = 0;
  where->root_expr = INVALID_EXPR_INDEX;
  where->use_expr_tree = false;
  
  // 檢查是否有括號，如果有就使用新的解析器
  bool has_parenthesis = false;
  for (char *p = clause_copy; *p != '\0'; p++) {
    if (*p == '(' || *p == ')') {
      has_parenthesis = true;
      break;
    }
  }
  
  if (has_parenthesis) {
    // 使用新的表達式樹解析器
    char *input = clause_copy;
    uint32_t root_idx;
    PrepareResult result = parse_where_expression(&input, where, &root_idx);
    if (result != PREPARE_SUCCESS) {
      return result;
    }
    where->root_expr = root_idx;
    where->use_expr_tree = true;
    
    // 向後兼容：如果只有一個基本條件，也設置舊的欄位
    if (where->num_expr_nodes == 1 && where->expr_nodes[0].type == WHERE_EXPR_BASIC) {
      where->field = where->expr_nodes[0].data.basic.field;
      where->op = where->expr_nodes[0].data.basic.op;
      if (where->expr_nodes[0].data.basic.field == WHERE_FIELD_ID) {
        where->value.id_value = where->expr_nodes[0].data.basic.value.id_value;
      } else {
        strncpy(where->value.string_value, where->expr_nodes[0].data.basic.value.string_value, 255);
        where->value.string_value[255] = '\0';
      }
    } else {
      where->field = WHERE_FIELD_NONE;
    }
    
    return PREPARE_SUCCESS;
  }
  
  // 舊的解析邏輯（沒有括號的情況）

  // 去除前後空格
  char *clause = clause_copy;
  while (*clause == ' ')
    clause++;

  // 解析多個條件
  char *token = strtok(clause, " ");
  uint32_t condition_idx = 0;

  while (token != NULL && condition_idx < MAX_WHERE_CONDITIONS) {
    // 解析欄位名稱
    char *field_name = token;
    WhereBasicCondition *current = &where->conditions[condition_idx];

    // 判斷欄位類型
    if (strcmp(field_name, "id") == 0) {
      current->field = WHERE_FIELD_ID;
    } else if (strcmp(field_name, "username") == 0) {
      current->field = WHERE_FIELD_USERNAME;
    } else if (strcmp(field_name, "email") == 0) {
      current->field = WHERE_FIELD_EMAIL;
    } else {
      printf("Error: Unknown field '%s' in WHERE clause (valid fields: id, username, email)\n", field_name);
      return PREPARE_SYNTAX_ERROR;
    }

    // 解析運算符
    char *op_string = strtok(NULL, " ");
    if (op_string == NULL) {
      printf("Error: WHERE clause missing operator after field '%s'\n", field_name);
      return PREPARE_SYNTAX_ERROR;
    }

    if (strcmp(op_string, "=") == 0) {
      current->op = WHERE_OP_EQUAL;
    } else if (strcmp(op_string, "!=") == 0) {
      current->op = WHERE_OP_NOT_EQUAL;
    } else if (strcmp(op_string, ">") == 0) {
      current->op = WHERE_OP_GREATER;
    } else if (strcmp(op_string, "<") == 0) {
      current->op = WHERE_OP_LESS;
    } else if (strcmp(op_string, ">=") == 0) {
      current->op = WHERE_OP_GREATER_EQUAL;
    } else if (strcmp(op_string, "<=") == 0) {
      current->op = WHERE_OP_LESS_EQUAL;
    } else {
      printf("Error: Invalid operator '%s' in WHERE clause (valid operators: =, !=, >, <, >=, <=)\n", op_string);
      return PREPARE_SYNTAX_ERROR;
    }

    // 解析值
    char *value_string = strtok(NULL, " ");
    if (value_string == NULL) {
      printf("Error: WHERE clause missing value after operator\n");
      return PREPARE_SYNTAX_ERROR;
    }

    // 根據欄位類型設定值
    if (current->field == WHERE_FIELD_ID) {
      int id = atoi(value_string);
      if (id < 0) {
        return PREPARE_NEGATIVE_ID;
      }
      current->value.id_value = (uint32_t)id;
    } else {
      if (strlen(value_string) > 255) {
        return PREPARE_STRING_TOO_LONG;
      }
      strncpy(current->value.string_value, value_string, 255);
      current->value.string_value[255] = '\0';
    }

    condition_idx++;
    where->num_conditions = condition_idx;

    // 檢查是否有邏輯運算符（AND/OR）
    token = strtok(NULL, " ");
    if (token != NULL) {
      if (strcmp(token, "and") == 0 || strcmp(token, "AND") == 0) {
        where->logical_ops[condition_idx - 1] = WHERE_LOGICAL_AND;
        token = strtok(NULL, " "); // 讀取下一個欄位名稱
      } else if (strcmp(token, "or") == 0 || strcmp(token, "OR") == 0) {
        where->logical_ops[condition_idx - 1] = WHERE_LOGICAL_OR;
        token = strtok(NULL, " "); // 讀取下一個欄位名稱
      } else {
        // 沒有邏輯運算符，可能是語法錯誤或已結束
        return PREPARE_SYNTAX_ERROR;
      }
    }
  }

  // 檢查是否有至少一個條件
  if (where->num_conditions == 0) {
    printf("Error: WHERE clause is empty\n");
    return PREPARE_SYNTAX_ERROR;
  }

  // 向後兼容：如果只有一個條件，也設置舊的欄位
  if (where->num_conditions == 1) {
    where->field = where->conditions[0].field;
    where->op = where->conditions[0].op;
    // 根據欄位類型複製值
    if (where->conditions[0].field == WHERE_FIELD_ID) {
      where->value.id_value = where->conditions[0].value.id_value;
    } else {
      strncpy(where->value.string_value, where->conditions[0].value.string_value, 255);
      where->value.string_value[255] = '\0';
    }
  } else {
    // 有多個條件時，標記為複雜條件
    where->field = WHERE_FIELD_NONE;
  }

  return PREPARE_SUCCESS;
}

/**
 * 評估單一基本條件是否滿足
 *
 * @param row Row 指標
 * @param condition WhereBasicCondition 指標
 * @return 是否滿足條件
 */
bool evaluate_basic_condition(Row *row, WhereBasicCondition *condition) {
  switch (condition->field) {
  case WHERE_FIELD_ID: {
    uint32_t row_value = row->id;
    uint32_t where_value = condition->value.id_value;

    switch (condition->op) {
    case WHERE_OP_EQUAL:
      return row_value == where_value;
    case WHERE_OP_NOT_EQUAL:
      return row_value != where_value;
    case WHERE_OP_GREATER:
      return row_value > where_value;
    case WHERE_OP_LESS:
      return row_value < where_value;
    case WHERE_OP_GREATER_EQUAL:
      return row_value >= where_value;
    case WHERE_OP_LESS_EQUAL:
      return row_value <= where_value;
    }
    break;
  }

  case WHERE_FIELD_USERNAME: {
    int cmp = strcmp(row->username, condition->value.string_value);

    switch (condition->op) {
    case WHERE_OP_EQUAL:
      return cmp == 0;
    case WHERE_OP_NOT_EQUAL:
      return cmp != 0;
    case WHERE_OP_GREATER:
      return cmp > 0;
    case WHERE_OP_LESS:
      return cmp < 0;
    case WHERE_OP_GREATER_EQUAL:
      return cmp >= 0;
    case WHERE_OP_LESS_EQUAL:
      return cmp <= 0;
    }
    break;
  }

  case WHERE_FIELD_EMAIL: {
    int cmp = strcmp(row->email, condition->value.string_value);

    switch (condition->op) {
    case WHERE_OP_EQUAL:
      return cmp == 0;
    case WHERE_OP_NOT_EQUAL:
      return cmp != 0;
    case WHERE_OP_GREATER:
      return cmp > 0;
    case WHERE_OP_LESS:
      return cmp < 0;
    case WHERE_OP_GREATER_EQUAL:
      return cmp >= 0;
    case WHERE_OP_LESS_EQUAL:
      return cmp <= 0;
    }
    break;
  }

  case WHERE_FIELD_NONE:
    return true;
  }

  return false;
}

/**
 * 評估表達式樹
 *
 * @param row Row 指標
 * @param where WhereCondition 指標
 * @param expr_idx 表達式索引
 * @return 是否滿足條件
 */
bool evaluate_expr_tree(Row *row, WhereCondition *where, uint32_t expr_idx) {
  if (expr_idx == INVALID_EXPR_INDEX || expr_idx >= where->num_expr_nodes) {
    return false;
  }
  
  WhereExprNode *node = &where->expr_nodes[expr_idx];
  
  switch (node->type) {
  case WHERE_EXPR_BASIC:
    return evaluate_basic_condition(row, &node->data.basic);
    
  case WHERE_EXPR_AND: {
    bool left_result = evaluate_expr_tree(row, where, node->data.logical.left);
    if (!left_result) {
      return false; // 短路評估
    }
    return evaluate_expr_tree(row, where, node->data.logical.right);
  }
    
  case WHERE_EXPR_OR: {
    bool left_result = evaluate_expr_tree(row, where, node->data.logical.left);
    if (left_result) {
      return true; // 短路評估
    }
    return evaluate_expr_tree(row, where, node->data.logical.right);
  }
  }
  
  return false;
}

/**
 * 評估 WHERE 條件是否滿足（支援複雜條件組合和括號）
 *
 * @param row Row 指標
 * @param where WhereCondition 指標
 * @return 是否滿足條件
 */
bool evaluate_where_condition(Row *row, WhereCondition *where) {
  // 如果使用表達式樹，使用新的評估邏輯
  if (where->use_expr_tree) {
    return evaluate_expr_tree(row, where, where->root_expr);
  }
  // 如果沒有複雜條件，使用舊的向後兼容邏輯
  if (where->num_conditions == 0) {
    // 沒有 WHERE 條件，返回 true
    if (where->field == WHERE_FIELD_NONE) {
      return true;
    }
    
    // 構建臨時的基本條件
    WhereBasicCondition temp_condition;
    temp_condition.field = where->field;
    temp_condition.op = where->op;
    // 根據欄位類型複製值
    if (where->field == WHERE_FIELD_ID) {
      temp_condition.value.id_value = where->value.id_value;
    } else {
      strncpy(temp_condition.value.string_value, where->value.string_value, 255);
      temp_condition.value.string_value[255] = '\0';
    }
    return evaluate_basic_condition(row, &temp_condition);
  }

  // 評估複雜條件組合
  bool result = evaluate_basic_condition(row, &where->conditions[0]);

  for (uint32_t i = 0; i < where->num_conditions - 1; i++) {
    bool next_result = evaluate_basic_condition(row, &where->conditions[i + 1]);

    switch (where->logical_ops[i]) {
    case WHERE_LOGICAL_AND:
      result = result && next_result;
      break;
    case WHERE_LOGICAL_OR:
      result = result || next_result;
      break;
    case WHERE_LOGICAL_NONE:
      // 不應該發生
      break;
    }
  }

  return result;
}

/**
 * 解析 SQL 語句
 *
 * @param input_buffer InputBuffer 指標
 * @param statement Statement 指標
 * @return 解析結果
 */
PrepareResult prepare_statement(InputBuffer *input_buffer,
                                Statement *statement) {
  if (strncmp(input_buffer->buffer, "insert", 6) == 0) {
    return prepare_insert(input_buffer, statement);
  }
  if (strncmp(input_buffer->buffer, "update", 6) == 0) {
    return prepare_update(input_buffer, statement);
  }
  if (strncmp(input_buffer->buffer, "delete", 6) == 0) {
    return prepare_delete(input_buffer, statement);
  }
  if (strncmp(input_buffer->buffer, "select", 6) == 0) {
    statement->type = STATEMENT_SELECT;
    statement->where.field = WHERE_FIELD_NONE;
    statement->where.num_conditions = 0; // 初始化條件數量
    statement->where.num_expr_nodes = 0;
    statement->where.root_expr = INVALID_EXPR_INDEX;
    statement->where.use_expr_tree = false;

    // 檢查是否有 WHERE 子句
    if (strlen(input_buffer->buffer) > 7) {
      char *remaining = input_buffer->buffer + 7; // 跳過 "select "
      // 去除前導空格
      while (*remaining == ' ')
        remaining++;

      if (strncmp(remaining, "where", 5) == 0) {
        char *where_clause = remaining + 5;
        // 去除 "where" 後的空格
        while (*where_clause == ' ')
          where_clause++;

        if (*where_clause != '\0') {
          return parse_where_clause(where_clause, &statement->where);
        }
      }
    }

    return PREPARE_SUCCESS;
  }

  return PREPARE_UNRECOGNIZED_STATEMENT;
}

/* ============================================================================
 * SQL 語句執行
 * ============================================================================
 */

/**
 * 執行 INSERT 語句
 *
 * @param statement Statement 指標
 * @param table Table 指標
 * @return 執行結果
 */
ExecuteResult execute_insert(Statement *statement, Table *table) {
  void *node = get_page_for_read(table, table->root_page_num);
  uint32_t num_cells = (*leaf_node_num_cells(node));

  Row *row_to_insert = &(statement->row_to_insert);
  uint32_t key_to_insert = row_to_insert->id;
  Cursor *cursor = table_find(table, key_to_insert);

  if (cursor->cell_num < num_cells) {
    uint32_t key_at_index = *leaf_node_key(node, cursor->cell_num);
    if (key_at_index == key_to_insert) {
      free(cursor);
      return EXECUTE_DUPLICATE_KEY;
    }
  }

  leaf_node_insert(cursor, row_to_insert->id, row_to_insert);
  free(cursor);
  
  // 更新統計資訊
  statistics_update_on_insert(table->statistics, row_to_insert);

  return EXECUTE_SUCCESS;
}

/**
 * 創建查詢計畫
 *
 * 根據 WHERE 條件分析並選擇最佳的查詢執行策略
 *
 * @param where WhereCondition 指標
 * @return QueryPlan 查詢計畫
 */
QueryPlan create_query_plan(WhereCondition *where) {
  QueryPlan plan;
  plan.type = QUERY_PLAN_FULL_SCAN;
  plan.has_start_key = false;
  plan.start_key = 0;
  plan.forward = true;
  plan.estimated_cost = 0.0;
  plan.estimated_rows = 0;

  // 如果沒有 WHERE 條件，使用全表掃描
  if (where->field == WHERE_FIELD_NONE && where->num_conditions == 0) {
    return plan;
  }

  // 檢查是否可以使用索引最佳化（單一條件且欄位為 id）
  if (where->field == WHERE_FIELD_ID && where->num_conditions == 0) {
    switch (where->op) {
    case WHERE_OP_EQUAL:
      // id = value：使用索引查找
      plan.type = QUERY_PLAN_INDEX_LOOKUP;
      plan.start_key = where->value.id_value;
      plan.has_start_key = true;
      break;

    case WHERE_OP_GREATER:
    case WHERE_OP_GREATER_EQUAL:
      // id > value 或 id >= value：從該位置開始正向掃描
      plan.type = QUERY_PLAN_RANGE_SCAN;
      if (where->op == WHERE_OP_GREATER) {
        plan.start_key = where->value.id_value + 1;
      } else {
        plan.start_key = where->value.id_value;
      }
      plan.has_start_key = true;
      plan.forward = true;
      break;

    case WHERE_OP_LESS:
    case WHERE_OP_LESS_EQUAL:
      // id < value 或 id <= value：從頭開始掃描直到該位置
      plan.type = QUERY_PLAN_RANGE_SCAN;
      plan.start_key = 0;
      plan.has_start_key = true;
      plan.forward = true;
      break;

    default:
      // 其他情況使用全表掃描
      plan.type = QUERY_PLAN_FULL_SCAN;
      break;
    }
  }
  // 檢查複雜條件中是否有 id 的簡單條件
  else if (where->num_conditions > 0) {
    // 尋找第一個 id = value 的條件
    for (uint32_t i = 0; i < where->num_conditions; i++) {
      if (where->conditions[i].field == WHERE_FIELD_ID &&
          where->conditions[i].op == WHERE_OP_EQUAL) {
        plan.type = QUERY_PLAN_INDEX_LOOKUP;
        plan.start_key = where->conditions[i].value.id_value;
        plan.has_start_key = true;
        break;
      }
    }
    // 如果沒有找到 id = value，尋找範圍條件
    if (plan.type == QUERY_PLAN_FULL_SCAN) {
      for (uint32_t i = 0; i < where->num_conditions; i++) {
        if (where->conditions[i].field == WHERE_FIELD_ID) {
          switch (where->conditions[i].op) {
          case WHERE_OP_GREATER:
          case WHERE_OP_GREATER_EQUAL:
            plan.type = QUERY_PLAN_RANGE_SCAN;
            if (where->conditions[i].op == WHERE_OP_GREATER) {
              plan.start_key = where->conditions[i].value.id_value + 1;
            } else {
              plan.start_key = where->conditions[i].value.id_value;
            }
            plan.has_start_key = true;
            plan.forward = true;
            break;
          default:
            break;
          }
          if (plan.type == QUERY_PLAN_RANGE_SCAN) {
            break;
          }
        }
      }
    }
  }

  plan.estimated_cost = 0.0;
  plan.estimated_rows = 0;
  return plan;
}

/* ============================================================================
 * 統計資訊管理
 * ============================================================================
 */

/**
 * 重置統計資訊
 *
 * @param stats TableStatistics 指標
 */
void statistics_reset(TableStatistics *stats) {
  stats->total_rows = 0;
  stats->id_min = UINT32_MAX;
  stats->id_max = 0;
  stats->id_cardinality = 0;
  stats->username_cardinality = 0;
  stats->email_cardinality = 0;
  stats->is_valid = false;
}

/**
 * 收集表的統計資訊
 *
 * @param table Table 指標
 * @return TableStatistics 指標，如果失敗返回 NULL
 */
TableStatistics *collect_table_statistics(Table *table) {
  TableStatistics *stats = malloc(sizeof(TableStatistics));
  if (stats == NULL) {
    return NULL;
  }
  
  statistics_reset(stats);
  
  // 使用簡單的哈希表來計算基數（使用位圖近似）
  // 為了簡化，我們使用一個簡單的集合來追蹤唯一值
  // 由於資源限制，我們使用採樣方法
  uint32_t unique_usernames = 0;
  uint32_t unique_emails = 0;
  uint32_t unique_ids = 0;
  
  // 簡單的哈希集合（使用布林陣列來近似）
  bool *username_seen = calloc(1024, sizeof(bool));
  bool *email_seen = calloc(1024, sizeof(bool));
  bool *id_seen = calloc(1024, sizeof(bool));
  
  if (!username_seen || !email_seen || !id_seen) {
    free(username_seen);
    free(email_seen);
    free(id_seen);
    free(stats);
    return NULL;
  }
  
  Cursor *cursor = table_start(table);
  Row row;
  
  while (!(cursor->end_of_table)) {
    deserialize_row(cursor_value(cursor), &row);
    
    stats->total_rows++;
    
    // 更新 ID 範圍
    if (row.id < stats->id_min) {
      stats->id_min = row.id;
    }
    if (row.id > stats->id_max) {
      stats->id_max = row.id;
    }
    
    // 簡單的哈希來計算基數（使用簡單的哈希函數）
    uint32_t username_hash = 0;
    for (int i = 0; row.username[i] != '\0' && i < COLUMN_USERNAME_SIZE; i++) {
      username_hash = (username_hash * 31 + row.username[i]) % 1024;
    }
    if (!username_seen[username_hash]) {
      username_seen[username_hash] = true;
      unique_usernames++;
    }
    
    uint32_t email_hash = 0;
    for (int i = 0; row.email[i] != '\0' && i < COLUMN_EMAIL_SIZE; i++) {
      email_hash = (email_hash * 31 + row.email[i]) % 1024;
    }
    if (!email_seen[email_hash]) {
      email_seen[email_hash] = true;
      unique_emails++;
    }
    
    uint32_t id_hash = row.id % 1024;
    if (!id_seen[id_hash]) {
      id_seen[id_hash] = true;
      unique_ids++;
    }
    
    cursor_advance(cursor);
  }
  
  free(cursor);
  free(username_seen);
  free(email_seen);
  free(id_seen);
  
  stats->id_cardinality = unique_ids;
  stats->username_cardinality = unique_usernames;
  stats->email_cardinality = unique_emails;
  stats->is_valid = true;
  
  // 如果表為空，重置統計資訊
  if (stats->total_rows == 0) {
    stats->id_min = UINT32_MAX;
    stats->id_max = 0;
  }
  
  return stats;
}

/**
 * 在插入時更新統計資訊
 *
 * @param stats TableStatistics 指標
 * @param row Row 指標
 */
void statistics_update_on_insert(TableStatistics *stats, Row *row) {
  if (!stats) return;
  
  stats->total_rows++;
  
  // 更新 ID 範圍
  if (row->id < stats->id_min) {
    stats->id_min = row->id;
  }
  if (row->id > stats->id_max) {
    stats->id_max = row->id;
  }
  
  // 基數更新：簡單遞增（實際應該檢查是否為新值）
  // 為簡化，我們假設每次插入都可能增加基數
  if (stats->id_cardinality < stats->total_rows) {
    stats->id_cardinality = stats->total_rows; // 近似值
  }
  
  stats->is_valid = true;
}

/**
 * 在刪除時更新統計資訊
 *
 * @param stats TableStatistics 指標
 * @param row Row 指標
 */
void statistics_update_on_delete(TableStatistics *stats, Row *row) {
  if (!stats || stats->total_rows == 0) return;
  
  (void)row; // 暫時未使用，保留參數以便將來擴展
  
  stats->total_rows--;
  
  // 如果表為空，重置統計資訊
  if (stats->total_rows == 0) {
    statistics_reset(stats);
    return;
  }
  
  // 基數更新：簡單遞減（實際應該檢查是否還有該值）
  if (stats->id_cardinality > stats->total_rows) {
    stats->id_cardinality = stats->total_rows; // 近似值
  }
  
  // 注意：ID 範圍不會自動更新，需要重新收集統計資訊
  // 但為了效能，我們保持當前值
}

/**
 * 從檔案載入統計資訊
 *
 * @param table Table 指標
 * @return 是否成功載入
 */
bool statistics_load(Table *table) {
  if (!table || !table->pager) return false;
  
  // 統計資訊檔案名稱：資料庫檔案名稱 + ".stats"
  char stats_filename[512];
  snprintf(stats_filename, sizeof(stats_filename), "%s.stats", 
           table->pager->file_descriptor == -1 ? "database" : "database");
  
  // 由於我們沒有檔案名稱，我們將統計資訊存儲在資料庫檔案的第一頁的特殊位置
  // 或者使用一個簡單的方法：檢查檔案是否存在
  // 為了簡化，我們暫時跳過持久化，每次啟動時重新收集
  
  return false; // 暫時返回 false，表示需要重新收集
}

/**
 * 保存統計資訊到檔案
 *
 * @param table Table 指標
 * @return 是否成功保存
 */
bool statistics_save(Table *table) {
  if (!table || !table->statistics || !table->statistics->is_valid) {
    return false;
  }
  
  // 由於我們沒有檔案名稱，我們暫時跳過持久化
  // 在實際應用中，應該將統計資訊保存到單獨的檔案或資料庫的特殊頁面
  
  return true; // 暫時返回 true
}

/* ============================================================================
 * 查詢成本估算
 * ============================================================================
 */

/**
 * 估算查詢結果的行數
 *
 * @param plan QueryPlan 指標
 * @param stats TableStatistics 指標
 * @param where WhereCondition 指標
 * @return 估算的結果行數
 */
uint32_t estimate_result_rows(QueryPlan *plan, TableStatistics *stats, WhereCondition *where) {
  if (!stats || !stats->is_valid || stats->total_rows == 0) {
    return 0;
  }
  
  uint32_t estimated = 0;
  
  switch (plan->type) {
  case QUERY_PLAN_INDEX_LOOKUP:
    // 索引查找：最多返回 1 行
    estimated = 1;
    break;
    
  case QUERY_PLAN_RANGE_SCAN:
    // 範圍掃描：根據 ID 範圍估算
    if (where->field == WHERE_FIELD_ID) {
      uint32_t start_id = plan->has_start_key ? plan->start_key : stats->id_min;
      uint32_t end_id = stats->id_max;
      
      if (where->op == WHERE_OP_LESS || where->op == WHERE_OP_LESS_EQUAL) {
        uint32_t value = where->value.id_value;
        if (where->op == WHERE_OP_LESS) {
          value--;
        }
        end_id = value < stats->id_max ? value : stats->id_max;
      } else if (where->op == WHERE_OP_GREATER || where->op == WHERE_OP_GREATER_EQUAL) {
        start_id = plan->start_key;
      }
      
      // 估算範圍內的行數（假設均勻分佈）
      if (stats->id_max > stats->id_min && end_id >= start_id) {
        double range_ratio = (double)(end_id - start_id + 1) / (double)(stats->id_max - stats->id_min + 1);
        estimated = (uint32_t)(stats->total_rows * range_ratio);
        if (estimated > stats->total_rows) {
          estimated = stats->total_rows;
        }
      }
    } else {
      // 非 ID 欄位的範圍掃描：假設返回 50% 的行
      estimated = stats->total_rows / 2;
    }
    break;
    
  case QUERY_PLAN_FULL_SCAN:
  default:
    // 全表掃描：需要根據 WHERE 條件估算選擇性
    if (where->field == WHERE_FIELD_NONE && where->num_conditions == 0) {
      // 沒有 WHERE 條件：返回所有行
      estimated = stats->total_rows;
    } else {
      // 有 WHERE 條件：根據欄位基數估算選擇性
      double selectivity = 1.0;
      
      if (where->field == WHERE_FIELD_ID) {
        // ID 欄位：選擇性 = 1 / 基數
        if (stats->id_cardinality > 0) {
          selectivity = 1.0 / stats->id_cardinality;
        }
      } else if (where->field == WHERE_FIELD_USERNAME) {
        // Username 欄位：選擇性 = 1 / 基數
        if (stats->username_cardinality > 0) {
          selectivity = 1.0 / stats->username_cardinality;
        }
      } else if (where->field == WHERE_FIELD_EMAIL) {
        // Email 欄位：選擇性 = 1 / 基數
        if (stats->email_cardinality > 0) {
          selectivity = 1.0 / stats->email_cardinality;
        }
      } else if (where->num_conditions > 0) {
        // 複雜條件：簡單估算為 10%
        selectivity = 0.1;
      }
      
      estimated = (uint32_t)(stats->total_rows * selectivity);
      if (estimated == 0 && stats->total_rows > 0) {
        estimated = 1; // 至少返回 1 行
      }
    }
    break;
  }
  
  return estimated;
}

/**
 * 估算查詢成本
 *
 * @param plan QueryPlan 指標
 * @param stats TableStatistics 指標
 * @param where WhereCondition 指標
 * @return 估算的成本
 */
double estimate_query_cost(QueryPlan *plan, TableStatistics *stats, WhereCondition *where) {
  if (!stats || !stats->is_valid || stats->total_rows == 0) {
    // 沒有統計資訊：使用固定成本
    switch (plan->type) {
    case QUERY_PLAN_INDEX_LOOKUP:
      return 1.0; // 索引查找：非常快
    case QUERY_PLAN_RANGE_SCAN:
      return 10.0; // 範圍掃描：中等成本
    case QUERY_PLAN_FULL_SCAN:
    default:
      return 100.0; // 全表掃描：高成本
    }
  }
  
  double cost = 0.0;
  uint32_t estimated_rows = estimate_result_rows(plan, stats, where);
  
  switch (plan->type) {
  case QUERY_PLAN_INDEX_LOOKUP:
    // 索引查找：O(log n) 成本
    cost = log2((double)stats->total_rows) + 1.0;
    break;
    
  case QUERY_PLAN_RANGE_SCAN:
    // 範圍掃描：O(log n + m)，其中 m 是結果行數
    cost = log2((double)stats->total_rows) + (double)estimated_rows;
    break;
    
  case QUERY_PLAN_FULL_SCAN:
  default:
    // 全表掃描：O(n)，但需要考慮 WHERE 條件評估的成本
    cost = (double)stats->total_rows;
    // 如果有 WHERE 條件，需要評估每行的成本
    if (where->field != WHERE_FIELD_NONE || where->num_conditions > 0) {
      cost += (double)stats->total_rows * 0.1; // 每行評估成本
    }
    break;
  }
  
  return cost;
}

/**
 * 使用統計資訊創建查詢計畫（進階版本）
 *
 * 根據統計資訊和成本估算選擇最佳的查詢執行策略
 *
 * @param where WhereCondition 指標
 * @param stats TableStatistics 指標
 * @return QueryPlan 查詢計畫
 */
QueryPlan create_query_plan_with_stats(WhereCondition *where, TableStatistics *stats) {
  QueryPlan best_plan;
  best_plan.type = QUERY_PLAN_FULL_SCAN;
  best_plan.has_start_key = false;
  best_plan.start_key = 0;
  best_plan.forward = true;
  best_plan.estimated_cost = 1000000.0; // 初始設為很大的值
  best_plan.estimated_rows = 0;
  
  // 如果沒有 WHERE 條件，使用全表掃描
  if (where->field == WHERE_FIELD_NONE && where->num_conditions == 0) {
    best_plan.type = QUERY_PLAN_FULL_SCAN;
    best_plan.estimated_rows = stats && stats->is_valid ? stats->total_rows : 0;
    best_plan.estimated_cost = estimate_query_cost(&best_plan, stats, where);
    return best_plan;
  }
  
  // 生成多個候選計畫並選擇成本最低的
  QueryPlan candidates[3];
  int candidate_count = 0;
  
  // 候選 1：索引查找（如果可能）
  if (where->field == WHERE_FIELD_ID && where->op == WHERE_OP_EQUAL && where->num_conditions == 0) {
    candidates[candidate_count].type = QUERY_PLAN_INDEX_LOOKUP;
    candidates[candidate_count].start_key = where->value.id_value;
    candidates[candidate_count].has_start_key = true;
    candidates[candidate_count].forward = true;
    candidate_count++;
  }
  
  // 候選 2：範圍掃描（如果可能）
  if (where->field == WHERE_FIELD_ID && where->num_conditions == 0) {
    if (where->op == WHERE_OP_GREATER || where->op == WHERE_OP_GREATER_EQUAL ||
        where->op == WHERE_OP_LESS || where->op == WHERE_OP_LESS_EQUAL) {
      candidates[candidate_count].type = QUERY_PLAN_RANGE_SCAN;
      if (where->op == WHERE_OP_GREATER || where->op == WHERE_OP_GREATER_EQUAL) {
        candidates[candidate_count].start_key = where->op == WHERE_OP_GREATER ? 
                                                 where->value.id_value + 1 : where->value.id_value;
        candidates[candidate_count].has_start_key = true;
        candidates[candidate_count].forward = true;
      } else {
        candidates[candidate_count].start_key = 0;
        candidates[candidate_count].has_start_key = true;
        candidates[candidate_count].forward = true;
      }
      candidate_count++;
    }
  }
  
  // 候選 3：全表掃描（總是可用）
  candidates[candidate_count].type = QUERY_PLAN_FULL_SCAN;
  candidates[candidate_count].has_start_key = false;
  candidates[candidate_count].forward = true;
  candidate_count++;
  
  // 評估每個候選計畫的成本並選擇最佳的
  for (int i = 0; i < candidate_count; i++) {
    candidates[i].estimated_rows = estimate_result_rows(&candidates[i], stats, where);
    candidates[i].estimated_cost = estimate_query_cost(&candidates[i], stats, where);
    
    if (candidates[i].estimated_cost < best_plan.estimated_cost) {
      best_plan = candidates[i];
    }
  }
  
  return best_plan;
}

/**
 * 執行 SELECT 語句（優化版本）
 *
 * 支援 WHERE 子句篩選和查詢最佳化
 * - 當 WHERE 條件是 id = value 時，使用索引查找
 * - 當 WHERE 條件是 id > value 或 id < value 時，使用範圍掃描
 * - 其他情況使用全表掃描
 *
 * @param statement Statement 指標
 * @param table Table 指標
 * @return 執行結果
 */
ExecuteResult execute_select(Statement *statement, Table *table) {
  // 使用統計資訊生成查詢計畫（如果統計資訊可用）
  QueryPlan plan;
  if (table->statistics && table->statistics->is_valid) {
    plan = create_query_plan_with_stats(&statement->where, table->statistics);
  } else {
    plan = create_query_plan(&statement->where);
    // 即使沒有統計資訊，也估算成本和行數
    plan.estimated_cost = estimate_query_cost(&plan, table->statistics, &statement->where);
    plan.estimated_rows = estimate_result_rows(&plan, table->statistics, &statement->where);
  }
  
  Cursor *cursor = NULL;
  Row row;

  switch (plan.type) {
  case QUERY_PLAN_INDEX_LOOKUP:
    // 索引查找：直接查找指定的 key
    cursor = table_find(table, plan.start_key);
    void *node = get_page_for_read(table, cursor->page_num);
    uint32_t num_cells = *leaf_node_num_cells(node);

    // 檢查是否找到該 key
    if (cursor->cell_num < num_cells) {
      uint32_t key_at_index = *leaf_node_key(node, cursor->cell_num);
      if (key_at_index == plan.start_key) {
        deserialize_row(cursor_value(cursor), &row);
        // 仍需評估完整的 WHERE 條件（可能有其他條件）
        if (evaluate_where_condition(&row, &statement->where)) {
          print_row(&row);
        }
      }
    }
    free(cursor);
    return EXECUTE_SUCCESS;

  case QUERY_PLAN_RANGE_SCAN:
    // 範圍掃描：從指定的 key 開始掃描
    if (plan.has_start_key && plan.start_key > 0) {
      cursor = table_find(table, plan.start_key);
    } else {
      cursor = table_start(table);
    }
    break;

  case QUERY_PLAN_FULL_SCAN:
  default:
    // 全表掃描：從頭開始掃描
    cursor = table_start(table);
    break;
  }

  // 執行掃描
  while (!(cursor->end_of_table)) {
    deserialize_row(cursor_value(cursor), &row);

    // 評估 WHERE 條件
    if (evaluate_where_condition(&row, &statement->where)) {
      print_row(&row);
    }

    cursor_advance(cursor);
  }

  free(cursor);
  return EXECUTE_SUCCESS;
}

/**
 * 執行 UPDATE 語句
 *
 * 支援部分欄位更新：只更新指定的欄位，保留其他欄位的原始值
 * 支援 WHERE 子句篩選
 *
 * @param statement Statement 指標
 * @param table Table 指標
 * @return 執行結果
 */
ExecuteResult execute_update(Statement *statement, Table *table) {
  Row *row_to_update = &(statement->row_to_insert);

  // 如果 WHERE 條件是簡單的 id = value，使用快速查找
  if (statement->where.field == WHERE_FIELD_ID &&
      statement->where.op == WHERE_OP_EQUAL) {
    uint32_t key_to_update = statement->where.value.id_value;

    // 使用 table_find 找到要更新的 key
    Cursor *cursor = table_find(table, key_to_update);
    void *node = get_page_for_write(table, cursor->page_num);
    uint32_t num_cells = *leaf_node_num_cells(node);

    // 檢查是否找到該 key
    if (cursor->cell_num < num_cells) {
      uint32_t key_at_index = *leaf_node_key(node, cursor->cell_num);
      if (key_at_index == key_to_update) {
        // 找到該 key，讀取現有資料
        Row existing_row;
        deserialize_row(leaf_node_value(node, cursor->cell_num), &existing_row);

        // 只更新指定的欄位
        if (statement->update_username) {
          strcpy(existing_row.username, row_to_update->username);
        }
        if (statement->update_email) {
          strcpy(existing_row.email, row_to_update->email);
        }

        // 將更新後的資料寫回
        serialize_row(&existing_row, leaf_node_value(node, cursor->cell_num));
        free(cursor);
        return EXECUTE_SUCCESS;
      }
    }

    // 沒有找到該 key
    free(cursor);
    return EXECUTE_KEY_NOT_FOUND;
  }

  // 其他 WHERE 條件，需要遍歷所有資料
  Cursor *cursor = table_start(table);
  Row row;
  bool found = false;

  while (!(cursor->end_of_table)) {
    void *node = get_page_for_write(table, cursor->page_num);
    deserialize_row(cursor_value(cursor), &row);

    // 評估 WHERE 條件
    if (evaluate_where_condition(&row, &statement->where)) {
      found = true;

      // 只更新指定的欄位
      if (statement->update_username) {
        strcpy(row.username, row_to_update->username);
      }
      if (statement->update_email) {
        strcpy(row.email, row_to_update->email);
      }

      // 將更新後的資料寫回
      serialize_row(&row, leaf_node_value(node, cursor->cell_num));
    }

    cursor_advance(cursor);
  }

  free(cursor);
  return found ? EXECUTE_SUCCESS : EXECUTE_KEY_NOT_FOUND;
}

/**
 * 執行 DELETE 語句
 *
 * 支援 WHERE 子句篩選
 *
 * @param statement Statement 指標
 * @param table Table 指標
 * @return 執行結果
 */
ExecuteResult execute_delete(Statement *statement, Table *table) {
  // 如果 WHERE 條件是簡單的 id = value，使用快速查找
  if (statement->where.field == WHERE_FIELD_ID &&
      statement->where.op == WHERE_OP_EQUAL) {
    uint32_t key_to_delete = statement->where.value.id_value;

    // 使用 table_find 找到要刪除的 key
    Cursor *cursor = table_find(table, key_to_delete);
    void *node = get_page(table->pager, cursor->page_num);
    uint32_t num_cells = *leaf_node_num_cells(node);

    // 檢查是否找到該 key
    if (cursor->cell_num < num_cells) {
      uint32_t key_at_index = *leaf_node_key(node, cursor->cell_num);
      if (key_at_index == key_to_delete) {
        // 找到該 key，讀取資料以更新統計資訊
        Row row_to_delete;
        deserialize_row(leaf_node_value(node, cursor->cell_num), &row_to_delete);
        
        // 執行刪除
        leaf_node_delete(cursor);
        
        // 更新統計資訊
        statistics_update_on_delete(table->statistics, &row_to_delete);
        
        free(cursor);
        return EXECUTE_SUCCESS;
      }
    }

    // 沒有找到該 key
    free(cursor);
    return EXECUTE_KEY_NOT_FOUND;
  }

  // 其他 WHERE 條件，需要遍歷所有資料
  // 先收集所有符合條件的 ID，然後再刪除
  uint32_t to_delete[1000]; // 假設最多刪除 1000 筆
  uint32_t delete_count = 0;

  Cursor *cursor = table_start(table);
  Row row;

  while (!(cursor->end_of_table) && delete_count < 1000) {
    deserialize_row(cursor_value(cursor), &row);

    // 評估 WHERE 條件
    if (evaluate_where_condition(&row, &statement->where)) {
      to_delete[delete_count++] = row.id;
    }

    cursor_advance(cursor);
  }
  free(cursor);

  // 如果沒有找到任何符合條件的資料
  if (delete_count == 0) {
    return EXECUTE_KEY_NOT_FOUND;
  }

  // 從後往前刪除，避免索引變化的問題
  for (int i = delete_count - 1; i >= 0; i--) {
    Cursor *delete_cursor = table_find(table, to_delete[i]);
    void *node = get_page(table->pager, delete_cursor->page_num);
    uint32_t num_cells = *leaf_node_num_cells(node);

    if (delete_cursor->cell_num < num_cells) {
      uint32_t key_at_index = *leaf_node_key(node, delete_cursor->cell_num);
      if (key_at_index == to_delete[i]) {
        // 讀取資料以更新統計資訊
        Row row_to_delete;
        deserialize_row(leaf_node_value(node, delete_cursor->cell_num), &row_to_delete);
        
        // 執行刪除
        leaf_node_delete(delete_cursor);
        
        // 更新統計資訊
        statistics_update_on_delete(table->statistics, &row_to_delete);
      }
    }

    free(delete_cursor);
  }

  return EXECUTE_SUCCESS;
}

/**
 * 執行 SQL 語句
 *
 * @param statement Statement 指標
 * @param table Table 指標
 * @return 執行結果
 */
ExecuteResult execute_statement(Statement *statement, Table *table) {
  switch (statement->type) {
  case STATEMENT_INSERT:
    return execute_insert(statement, table);
  case STATEMENT_SELECT:
    return execute_select(statement, table);
  case STATEMENT_UPDATE:
    return execute_update(statement, table);
  case STATEMENT_DELETE:
    return execute_delete(statement, table);
  }
  return EXECUTE_SUCCESS;
}

/* ============================================================================
 * REPL 主程式
 * ============================================================================
 */

/**
 * 主程式：REPL（Read-Eval-Print Loop）
 *
 * @param argc 參數數量
 * @param argv 參數陣列
 * @return 程式結束碼
 */
int main(int argc, char *argv[]) {
  if (argc < 2) {
    printf("Must supply a database filename.\n");
    exit(EXIT_FAILURE);
  }

  char *filename = argv[1];
  Table *table = db_open(filename);

  InputBuffer *input_buffer = new_input_buffer();
  while (true) {
    print_prompt();
    read_input(input_buffer);

    if (input_buffer->buffer[0] == '.') {
      switch (do_meta_command(input_buffer, table)) {
      case META_COMMAND_SUCCESS:
        continue;
      case META_COMMAND_UNRECOGNIZED_COMMAND:
        printf("Unrecognized command '%s'\n", input_buffer->buffer);
        continue;
      }
    }
    
    // 處理交易命令（不區分大小寫）
    char *cmd_lower = strdup(input_buffer->buffer);
    for (char *p = cmd_lower; *p; p++) {
      *p = tolower(*p);
    }
    
    if (strcmp(cmd_lower, "begin") == 0 || strcmp(cmd_lower, "begin transaction") == 0) {
      if (transaction_begin(table)) {
        printf("Transaction started.\n");
      }
      free(cmd_lower);
      continue;
    } else if (strcmp(cmd_lower, "commit") == 0) {
      if (transaction_commit(table) == EXECUTE_SUCCESS) {
        printf("Transaction committed.\n");
      }
      free(cmd_lower);
      continue;
    } else if (strcmp(cmd_lower, "rollback") == 0) {
      if (transaction_rollback(table) == EXECUTE_SUCCESS) {
        printf("Transaction rolled back.\n");
      }
      free(cmd_lower);
      continue;
    } else if (strcmp(cmd_lower, "analyze") == 0) {
      // 處理 ANALYZE 命令（不區分大小寫）
      printf("Analyzing table statistics...\n");
      TableStatistics *new_stats = collect_table_statistics(table);
      if (new_stats != NULL) {
        memcpy(table->statistics, new_stats, sizeof(TableStatistics));
        free(new_stats);
        statistics_save(table);
        printf("Statistics updated successfully.\n");
        printf("  Total rows: %u\n", table->statistics->total_rows);
        printf("  ID range: %u - %u\n", table->statistics->id_min, table->statistics->id_max);
        printf("  ID cardinality: %u\n", table->statistics->id_cardinality);
        printf("  Username cardinality: %u\n", table->statistics->username_cardinality);
        printf("  Email cardinality: %u\n", table->statistics->email_cardinality);
      } else {
        printf("Error: Failed to collect statistics.\n");
      }
      free(cmd_lower);
      continue;
    }
    free(cmd_lower);

    Statement statement;
    switch (prepare_statement(input_buffer, &statement)) {
    case PREPARE_SUCCESS:
      break;
    case PREPARE_NEGATIVE_ID:
      // 錯誤訊息已在 prepare 函數中輸出
      continue;
    case PREPARE_STRING_TOO_LONG:

      continue;
    case PREPARE_SYNTAX_ERROR:
     
      continue;
    case PREPARE_UNRECOGNIZED_STATEMENT:
      printf("Error: Unrecognized command '%s'\n", input_buffer->buffer);
      continue;
    }

    switch (execute_statement(&statement, table)) {
    case EXECUTE_SUCCESS:
      printf("Executed.\n");
      break;
    case EXECUTE_DUPLICATE_KEY:
      printf("Error: Duplicate key.\n");
      break;
    case EXECUTE_TABLE_FULL:
      printf("Error: Table full.\n");
      break;
    case EXECUTE_KEY_NOT_FOUND:
      printf("Error: Key not found.\n");
      break;
    }
  }
}
