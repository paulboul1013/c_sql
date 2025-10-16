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

#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>

/* ============================================================================
 * 常數定義
 * ============================================================================ */

#define TABLE_MAX_PAGES 100
#define INVALID_PAGE_NUM UINT32_MAX
#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255

/* ============================================================================
 * 型別定義與資料結構
 * ============================================================================ */

// B-tree 節點類型
typedef enum { 
    NODE_INTERNAL,  // 內部節點（索引）
    NODE_LEAF       // 葉節點（資料）
} NodeType;

// 執行結果狀態
typedef enum {
    EXECUTE_SUCCESS,
    EXECUTE_DUPLICATE_KEY,
    EXECUTE_TABLE_FULL,
    EXECUTE_KEY_NOT_FOUND
} ExecuteResult;

// SQL 語句類型
typedef enum {
    STATEMENT_INSERT,
    STATEMENT_SELECT,
    STATEMENT_UPDATE,
    STATEMENT_DELETE
} StatementType;

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
    Row row_to_insert;  // 用於 INSERT、UPDATE 和 DELETE (DELETE 只使用 id 欄位)
    // 用於 UPDATE 語句的欄位更新標識
    bool update_username;  // 是否更新 username
    bool update_email;     // 是否更新 email
} Statement;

// 頁面管理器
typedef struct {
    int file_descriptor;
    uint32_t file_length;
    uint32_t num_pages;
    void* pages[TABLE_MAX_PAGES];
} Pager;

// 資料表結構
typedef struct {
    Pager *pager;
    uint32_t root_page_num;
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
 * ============================================================================ */

#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)

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
 * ============================================================================ */

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
const uint32_t COMMON_NODE_HEADER_SIZE = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

/*
 * 葉節點標頭（Leaf Node Header）：
 * - num_cells: 儲存的資料筆數
 * - next_leaf: 下一個葉節點的頁面編號（用於順序遍歷）
 */
const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_NEXT_LEAF_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NEXT_LEAF_OFFSET = LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE;
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
const uint32_t LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const uint32_t LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const uint32_t LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

// 葉節點分裂時的分配數量
const uint32_t LEAF_NODE_RIGHT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) / 2;
const uint32_t LEAF_NODE_LEFT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_RIGHT_SPLIT_COUNT;

/*
 * 內部節點標頭（Internal Node Header）：
 * - num_keys: 鍵的數量
 * - right_child: 最右側子節點的頁面編號
 */
const uint32_t INTERNAL_NODE_NUM_KEYS_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_NUM_KEYS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t INTERNAL_NODE_RIGHT_CHILD_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_RIGHT_CHILD_OFFSET = INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE;
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
const uint32_t INTERNAL_NODE_CELL_SIZE = INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE;

// 為了測試方便，暫時設定較小的值
const uint32_t INTERNAL_NODE_MAX_CELLS = 3;

/* ============================================================================
 * 函式前置宣告
 * ============================================================================ */

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
void leaf_node_merge(Table *table, uint32_t left_page_num, uint32_t right_page_num);
void internal_node_merge(Table *table, uint32_t parent_page_num, uint32_t left_page_num, uint32_t right_page_num);
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
void internal_node_insert(Table *table, uint32_t parent_page_num, uint32_t child_page_num);
void internal_node_split_and_insert(Table *table, uint32_t parent_page_num, uint32_t child_page_num);
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

// 命令處理
InputBuffer *new_input_buffer(void);
void close_input_buffer(InputBuffer *input_buffer);
MetaCommandResult do_meta_command(InputBuffer *input_buffer, Table *table);
PrepareResult prepare_statement(InputBuffer *input_buffer, Statement *statement);
PrepareResult prepare_insert(InputBuffer *input_buffer, Statement *statement);
PrepareResult prepare_update(InputBuffer *input_buffer, Statement *statement);
PrepareResult prepare_delete(InputBuffer *input_buffer, Statement *statement);
ExecuteResult execute_statement(Statement *statement, Table *table);
ExecuteResult execute_insert(Statement *statement, Table *table);
ExecuteResult execute_select(Statement *statement, Table *table);
ExecuteResult execute_update(Statement *statement, Table *table);
ExecuteResult execute_delete(Statement *statement, Table *table);

/* ============================================================================
 * 輔助與工具函式
 * ============================================================================ */

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
uint32_t get_unused_page_num(Pager *pager) {
    return pager->num_pages;
}

/* ============================================================================
 * Pager 管理（檔案 I/O 與頁面快取）
 * ============================================================================ */

/**
 * 開啟資料庫檔案，初始化 Pager
 * 
 * @param filename 資料庫檔案路徑
 * @return 初始化完成的 Pager 指標
 */
Pager *pager_open(const char *filename) {
    int fd = open(filename,
                  O_RDWR | O_CREAT,
                  S_IRUSR | S_IWUSR);

    if (fd == -1) {
        printf("Unable to open file\n");
                exit(EXIT_FAILURE);
            }
                
    off_t file_length = lseek(fd, 0, SEEK_END);
    
    Pager *pager = malloc(sizeof(Pager));
    pager->file_descriptor = fd;
    pager->file_length = file_length;
    pager->num_pages = (file_length / PAGE_SIZE);

    if (file_length % PAGE_SIZE != 0) {
        printf("Db file is not a whole number of pages. Corrupt file.\n");
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
        printf("Tried to fetch page number out of bounds. %u > %d\n", page_num, TABLE_MAX_PAGES);
        exit(EXIT_FAILURE);
    }

    if (pager->pages[page_num] == NULL) {
        // Cache miss：配置記憶體並從檔案載入
        void *page = malloc(PAGE_SIZE);
        uint32_t num_pages = pager->file_length / PAGE_SIZE;

        // 檔案末端可能有部分頁面
        if (pager->file_length % PAGE_SIZE) {
            num_pages += 1;
        }

        if (page_num <= num_pages) {
            lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
            ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);
            if (bytes_read == -1) {
                printf("Error reading file: %d\n", errno);
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
        printf("Tried to flush null page\n");
        exit(EXIT_FAILURE);
    }

    off_t offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
    if (offset == -1) {
        printf("Error seeking: %d\n", errno);
        exit(EXIT_FAILURE);
    }

    ssize_t bytes_written = write(pager->file_descriptor, pager->pages[page_num], PAGE_SIZE);
    if (bytes_written == -1) {
        printf("Error writing: %d\n", errno);
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
    table->pager = pager;
    table->root_page_num = 0;

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
        printf("Error closing db file\n");
        exit(EXIT_FAILURE);
    }

    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
        void *page = pager->pages[i];
        if (page) {
            free(page);
            pager->pages[i] = NULL;
        }
    }

    free(pager);
    free(table);
}

/* ============================================================================
 * 節點操作（通用）
 * ============================================================================ */

/**
 * 取得節點類型
 */
NodeType get_node_type(void *node) {
    uint8_t value = *((uint8_t*)(node + NODE_TYPE_OFFSET));
    return (NodeType)value;
}

/**
 * 設定節點類型
 */
void set_node_type(void *node, NodeType type) {
    uint8_t value = type;
    *((uint8_t*)(node + NODE_TYPE_OFFSET)) = value;
}

/**
 * 檢查節點是否為根節點
 */
bool is_node_root(void *node) {
    uint8_t value = *((uint8_t*)(node + IS_ROOT_OFFSET));
    return (bool)value;
}

/**
 * 設定節點的根節點旗標
 */
void set_node_root(void *node, bool is_root) {
    uint8_t value = is_root;
    *((uint8_t*)(node + IS_ROOT_OFFSET)) = value;
}

/**
 * 取得節點的父節點頁面編號指標
 */
uint32_t *node_parent(void *node) {
    return node + PARENT_POINTER_OFFSET;
}

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
 * ============================================================================ */

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
    *leaf_node_next_leaf(node) = 0;  // 0 表示沒有下一個葉節點
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
    void *node = get_page(table->pager, page_num);
    uint32_t num_cells = *leaf_node_num_cells(node);

    Cursor *cursor = malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->page_num = page_num;

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
    void *node = get_page(cursor->table->pager, cursor->page_num);
    uint32_t num_cells = *leaf_node_num_cells(node);

    if (num_cells >= LEAF_NODE_MAX_CELLS) {
        // 節點已滿，需要分裂
        leaf_node_split_and_insert(cursor, key, value);
        return;
    }

    if (cursor->cell_num < num_cells) {
        // 為新 cell 騰出空間
        for (uint32_t i = num_cells; i > cursor->cell_num; i--) {
            memcpy(leaf_node_cell(node, i), leaf_node_cell(node, i - 1), LEAF_NODE_CELL_SIZE);
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
    void *node = get_page(cursor->table->pager, cursor->page_num);
    uint32_t num_cells = *leaf_node_num_cells(node);

    if (cursor->cell_num >= num_cells) {
        // 超出範圍，不應該發生
        return;
    }

    // 將後面的 cell 向前移動以填補刪除的空缺
    for (uint32_t i = cursor->cell_num; i < num_cells - 1; i++) {
        memcpy(leaf_node_cell(node, i), leaf_node_cell(node, i + 1), LEAF_NODE_CELL_SIZE);
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
            uint32_t left_sibling_page = *internal_node_child(parent, child_index - 1);
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
            uint32_t left_sibling_page = *internal_node_child(parent, child_index - 1);
            void *left_sibling = get_page(table->pager, left_sibling_page);
            uint32_t left_cells = *leaf_node_num_cells(left_sibling);
            
            // 如果兩個節點的 cell 總數不超過最大容量，可以合併
            if (num_cells + left_cells <= LEAF_NODE_MAX_CELLS) {
                return true;
            }
        }
        
        // 檢查右兄弟節點
        if (child_index < num_keys) {
            uint32_t right_sibling_page = *internal_node_child(parent, child_index + 1);
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
void leaf_node_merge(Table *table, uint32_t left_page_num, uint32_t right_page_num) {
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
            uint32_t left_sibling_page = *internal_node_child(parent, child_index - 1);
            void *left_sibling = get_page(table->pager, left_sibling_page);
            uint32_t left_keys = *internal_node_num_keys(left_sibling);
            
            // 如果兩個節點的鍵總數不超過最大容量，可以合併
            if (num_keys + left_keys + 1 <= INTERNAL_NODE_MAX_CELLS) {
                return true;
            }
        }
        
        // 檢查右兄弟節點
        if (child_index < parent_num_keys) {
            uint32_t right_sibling_page = *internal_node_child(parent, child_index + 1);
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
void internal_node_merge(Table *table, uint32_t parent_page_num, uint32_t left_page_num, uint32_t right_page_num) {
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
    *internal_node_child(left_node, left_keys + 1) = *internal_node_child(right_node, 0);
    
    // 將右節點的所有鍵和子節點移到左節點
    for (uint32_t i = 0; i < right_keys; i++) {
        *internal_node_key(left_node, left_keys + 1 + i) = *internal_node_key(right_node, i);
        *internal_node_child(left_node, left_keys + 2 + i) = *internal_node_child(right_node, i + 1);
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
            serialize_row(value, leaf_node_value(destination_node, index_within_node));
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
 * ============================================================================ */

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
static inline uint32_t *internal_node_child_cell(void *node, uint32_t cell_num) {
    return node + INTERNAL_NODE_HEADER_SIZE + cell_num * INTERNAL_NODE_CELL_SIZE;
}

/**
 * 取得內部節點第 cell_num 個 cell 的位址
 */
void *internal_node_cell(void *node, uint32_t cell_num) {
    return (void*)(node + INTERNAL_NODE_HEADER_SIZE + cell_num * INTERNAL_NODE_CELL_SIZE);
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
            printf("Tried to access child %u of node, but was invalid page\n", child_num);
            exit(EXIT_FAILURE);
        }
        return child;
    }
}

/**
 * 取得內部節點第 key_num 個 key 的指標
 */
uint32_t *internal_node_key(void *node, uint32_t key_num) {
    return (void*)internal_node_cell(node, key_num) + INTERNAL_NODE_CHILD_SIZE;
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
    uint32_t max_index = num_keys;  // 子節點數量比 key 數量多 1

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
void internal_node_insert(Table *table, uint32_t parent_page_num, uint32_t child_page_num) {
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
        *internal_node_key(parent, original_num_keys) = get_node_max_key(table->pager, right_child);
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
void internal_node_split_and_insert(Table *table, uint32_t parent_page_num, uint32_t child_page_num) {
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
    for (int i = INTERNAL_NODE_MAX_CELLS - 1; i > INTERNAL_NODE_MAX_CELLS / 2; i--) {
        cur_page_num = *internal_node_child(old_node, i);
        cur = get_page(table->pager, cur_page_num);
        internal_node_insert(table, new_page_num, cur_page_num);
        *node_parent(cur) = new_page_num;
        
        // 修正：正確地遞減 key 數量
        (*old_num_keys) = (*old_num_keys) - 1;
    }

    // 將最後一個 child 設為 old_node 的 right_child
    *internal_node_right_child(old_node) = *internal_node_child(old_node, (*old_num_keys) - 1);
    (*old_num_keys) = (*old_num_keys) - 1;

    uint32_t max_after_split = get_node_max_key(table->pager, old_node);

    // 決定新 child 應插入到哪個節點
    uint32_t destination_page_num = child_max < max_after_split ? old_page_num : new_page_num;
    internal_node_insert(table, destination_page_num, child_page_num);
    *node_parent(child) = destination_page_num;

    update_internal_node_key(parent, old_max, get_node_max_key(table->pager, old_node));

    if (!splitting_root) {
        internal_node_insert(table, *node_parent(old_node), new_page_num);
        *node_parent(new_node) = *node_parent(old_node);
    }
}

/* ============================================================================
 * B-Tree 分裂與重組
 * ============================================================================ */

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
 * ============================================================================ */

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
    void *node = get_page(table->pager, cursor->page_num);
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
    void *page = get_page(cursor->table->pager, page_num);
    return leaf_node_value(page, cursor->cell_num);
}

/**
 * 將 cursor 移動到下一筆資料
 * 
 * @param cursor Cursor 指標
 */
void cursor_advance(Cursor *cursor) {
    uint32_t page_num = cursor->page_num;
    void *node = get_page(cursor->table->pager, page_num);

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
 * ============================================================================ */

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
 * ============================================================================ */

/**
 * 創建新的輸入緩衝區
 * 
 * @return InputBuffer 指標
 */
InputBuffer *new_input_buffer(void) {
    InputBuffer *input_buffer = malloc(sizeof(InputBuffer));
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
void print_prompt(void) {
    printf("db > ");
}

/**
 * 讀取使用者輸入
 * 
 * @param input_buffer InputBuffer 指標
 */
void read_input(InputBuffer *input_buffer) {
    ssize_t bytes_read = getline(&(input_buffer->buffer), 
                                  &(input_buffer->buffer_length), 
                                  stdin);

    if (bytes_read <= 0) {
        printf("Error reading input\n");
        exit(EXIT_FAILURE);
    }

    // 移除尾端的換行符號
    input_buffer->input_length = bytes_read - 1;
    input_buffer->buffer[bytes_read - 1] = 0;
}

/* ============================================================================
 * 元命令處理（以 . 開頭的命令）
 * ============================================================================ */

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
    } else {
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}

/* ============================================================================
 * SQL 語句解析
 * ============================================================================ */

/**
 * 解析 INSERT 語句
 * 
 * @param input_buffer InputBuffer 指標
 * @param statement Statement 指標
 * @return 解析結果
 */
PrepareResult prepare_insert(InputBuffer *input_buffer, Statement *statement) {
    statement->type = STATEMENT_INSERT;

    char *keyword = strtok(input_buffer->buffer, " ");
    char *id_string = strtok(NULL, " ");
    char *username = strtok(NULL, " ");
    char *email = strtok(NULL, " ");

    if (id_string == NULL || username == NULL || email == NULL) {
        return PREPARE_SYNTAX_ERROR;
    }

    int id = atoi(id_string);
    if (id < 0) {
        return PREPARE_NEGATIVE_ID;
    }

    if (strlen(username) > COLUMN_USERNAME_SIZE) {
        return PREPARE_STRING_TOO_LONG;
    }

    if (strlen(email) > COLUMN_EMAIL_SIZE) {
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
 * 語法：update [id] [username] [email]
 * 使用 '-' 表示不更新該欄位
 * 
 * 範例：
 *   update 1 new_username new_email@example.com  (更新兩個欄位)
 *   update 1 - new_email@example.com             (只更新 email)
 *   update 1 new_username -                      (只更新 username)
 * 
 * @param input_buffer InputBuffer 指標
 * @param statement Statement 指標
 * @return 解析結果
 */
PrepareResult prepare_update(InputBuffer *input_buffer, Statement *statement) {
    statement->type = STATEMENT_UPDATE;
    statement->update_username = false;
    statement->update_email = false;

    char *keyword = strtok(input_buffer->buffer, " ");
    char *id_string = strtok(NULL, " ");
    char *username = strtok(NULL, " ");
    char *email = strtok(NULL, " ");

    if (id_string == NULL || username == NULL || email == NULL) {
        return PREPARE_SYNTAX_ERROR;
    }

    int id = atoi(id_string);
    if (id < 0) {
        return PREPARE_NEGATIVE_ID;
    }

    // 檢查是否要更新 username（'-' 表示不更新）
    if (strcmp(username, "-") != 0) {
        if (strlen(username) > COLUMN_USERNAME_SIZE) {
            return PREPARE_STRING_TOO_LONG;
        }
        strcpy(statement->row_to_insert.username, username);
        statement->update_username = true;
    }

    // 檢查是否要更新 email（'-' 表示不更新）
    if (strcmp(email, "-") != 0) {
        if (strlen(email) > COLUMN_EMAIL_SIZE) {
            return PREPARE_STRING_TOO_LONG;
        }
        strcpy(statement->row_to_insert.email, email);
        statement->update_email = true;
    }

    statement->row_to_insert.id = id;

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

    char *keyword = strtok(input_buffer->buffer, " ");
    char *id_string = strtok(NULL, " ");

    if (id_string == NULL) {
        return PREPARE_SYNTAX_ERROR;
    }

    int id = atoi(id_string);
    if (id < 0) {
        return PREPARE_NEGATIVE_ID;
    }

    statement->row_to_insert.id = id;

    return PREPARE_SUCCESS;
}

/**
 * 解析 SQL 語句
 * 
 * @param input_buffer InputBuffer 指標
 * @param statement Statement 指標
 * @return 解析結果
 */
PrepareResult prepare_statement(InputBuffer *input_buffer, Statement *statement) {
    if (strncmp(input_buffer->buffer, "insert", 6) == 0) {
        return prepare_insert(input_buffer, statement);
    }
    if (strncmp(input_buffer->buffer, "update", 6) == 0) {
        return prepare_update(input_buffer, statement);
    }
    if (strncmp(input_buffer->buffer, "delete", 6) == 0) {
        return prepare_delete(input_buffer, statement);
    }
    if (strcmp(input_buffer->buffer, "select") == 0) {
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }

    return PREPARE_UNRECOGNIZED_STATEMENT;
}

/* ============================================================================
 * SQL 語句執行
 * ============================================================================ */

/**
 * 執行 INSERT 語句
 * 
 * @param statement Statement 指標
 * @param table Table 指標
 * @return 執行結果
 */
ExecuteResult execute_insert(Statement *statement, Table *table) {
    void *node = get_page(table->pager, table->root_page_num);
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

    return EXECUTE_SUCCESS;
}

/**
 * 執行 SELECT 語句
 * 
 * @param statement Statement 指標
 * @param table Table 指標
 * @return 執行結果
 */
ExecuteResult execute_select(Statement *statement, Table *table) {
    (void)statement;  // 未使用的參數
    
    Cursor *cursor = table_start(table);
    Row row;
    
    while (!(cursor->end_of_table)) {
        deserialize_row(cursor_value(cursor), &row);
        print_row(&row);
        cursor_advance(cursor);
    }

    free(cursor);
    return EXECUTE_SUCCESS;
}

/**
 * 執行 UPDATE 語句
 * 
 * 支援部分欄位更新：只更新指定的欄位，保留其他欄位的原始值
 * 
 * @param statement Statement 指標
 * @param table Table 指標
 * @return 執行結果
 */
ExecuteResult execute_update(Statement *statement, Table *table) {
    Row *row_to_update = &(statement->row_to_insert);
    uint32_t key_to_update = row_to_update->id;
    
    // 使用 table_find 找到要更新的 key
    Cursor *cursor = table_find(table, key_to_update);
    void *node = get_page(table->pager, cursor->page_num);
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

/**
 * 執行 DELETE 語句
 * 
 * @param statement Statement 指標
 * @param table Table 指標
 * @return 執行結果
 */
ExecuteResult execute_delete(Statement *statement, Table *table) {
    uint32_t key_to_delete = statement->row_to_insert.id;
    
    // 使用 table_find 找到要刪除的 key
    Cursor *cursor = table_find(table, key_to_delete);
    void *node = get_page(table->pager, cursor->page_num);
    uint32_t num_cells = *leaf_node_num_cells(node);
    
    // 檢查是否找到該 key
    if (cursor->cell_num < num_cells) {
        uint32_t key_at_index = *leaf_node_key(node, cursor->cell_num);
        if (key_at_index == key_to_delete) {
            // 找到該 key，執行刪除
            leaf_node_delete(cursor);
            free(cursor);
            return EXECUTE_SUCCESS;
        }
    }
    
    // 沒有找到該 key
    free(cursor);
    return EXECUTE_KEY_NOT_FOUND;
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
 * ============================================================================ */

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

        Statement statement;
        switch (prepare_statement(input_buffer, &statement)) {
            case PREPARE_SUCCESS:
                break;
            case PREPARE_NEGATIVE_ID:
                printf("ID must be positive.\n");
                continue;
            case PREPARE_STRING_TOO_LONG:
                printf("String is too long.\n");
                continue;
            case PREPARE_SYNTAX_ERROR:
                printf("Syntax error. Could not parse statement.\n");
                continue;
            case PREPARE_UNRECOGNIZED_STATEMENT:
                printf("Unrecognized keyword at start of '%s'\n", input_buffer->buffer);
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
