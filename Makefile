# ============================================================================
# C-SQL Makefile
# ============================================================================

# 編譯器設定
CC = gcc
CFLAGS = -std=c11 -Wall -Wextra -O2
CFLAGS_DEBUG = -std=c11 -Wall -Wextra -g -DDEBUG

# 目標檔案
TARGET = main
SOURCE = main.c

# 測試相關
TEST_SCRIPT = test_db.py
PYTHON = python3

# 顏色輸出（可選）
GREEN = \033[0;32m
YELLOW = \033[0;33m
RED = \033[0;31m
NC = \033[0m # No Color

# ============================================================================
# 主要目標
# ============================================================================

.PHONY: all clean test debug help install

# 預設目標
all: $(TARGET)
	@echo "$(GREEN)✓ 編譯完成！$(NC)"
	@echo "執行方式: ./$(TARGET) <database_file>"

# 編譯主程式（Release 版本）
$(TARGET): $(SOURCE)
	@echo "$(YELLOW)正在編譯 $(TARGET) (Release)...$(NC)"
	$(CC) $(CFLAGS) $(SOURCE) -o $(TARGET)

# 編譯除錯版本
debug: $(SOURCE)
	@echo "$(YELLOW)正在編譯 $(TARGET) (Debug)...$(NC)"
	$(CC) $(CFLAGS_DEBUG) $(SOURCE) -o $(TARGET)
	@echo "$(GREEN)✓ 除錯版本編譯完成！$(NC)"

# ============================================================================
# 測試相關
# ============================================================================

# 執行所有測試
test: $(TARGET)
	@echo "$(YELLOW)執行測試套件...$(NC)"
	@$(PYTHON) $(TEST_SCRIPT)
	@echo "$(GREEN)✓ 測試完成！$(NC)"


# 快速測試（只測試基本功能）
test-quick: $(TARGET)
	@echo "$(YELLOW)執行快速測試...$(NC)"
	@rm -f quick_test.db
	@echo "insert 1 test test@example.com\nselect\nselect where id = 1\n.exit" | ./$(TARGET) quick_test.db
	@rm -f quick_test.db
	@echo "$(GREEN)✓ 快速測試通過！$(NC)"

# ============================================================================
# 清理相關
# ============================================================================

# 清理編譯產物
clean:
	@echo "$(YELLOW)清理編譯產物...$(NC)"
	@rm -f $(TARGET)
	@echo "$(GREEN)✓ 清理完成！$(NC)"

# 清理所有檔案（包括測試資料庫）
clean-all: clean
	@echo "$(YELLOW)清理所有檔案...$(NC)"
	@rm -f *.db
	@rm -f mydb.db persistence_test.db large_dataset.db
	@rm -f where_test.db where_edge_test.db where_perf_test.db
	@echo "$(GREEN)✓ 全部清理完成！$(NC)"

# ============================================================================
# 開發輔助
# ============================================================================

# 檢查程式碼風格（使用 cppcheck）
check:
	@echo "$(YELLOW)檢查程式碼...$(NC)"
	@if command -v cppcheck > /dev/null; then \
		cppcheck --enable=all --suppress=missingIncludeSystem $(SOURCE); \
	else \
		echo "$(RED)未安裝 cppcheck，跳過檢查$(NC)"; \
	fi

# 格式化程式碼（使用 clang-format）
format:
	@echo "$(YELLOW)格式化程式碼...$(NC)"
	@if command -v clang-format > /dev/null; then \
		clang-format -i $(SOURCE); \
		echo "$(GREEN)✓ 格式化完成！$(NC)"; \
	else \
		echo "$(RED)未安裝 clang-format，跳過格式化$(NC)"; \
	fi

# 重新編譯
rebuild: clean all

# 重新編譯並測試
rebuild-test: clean all test

# ============================================================================
# 執行相關
# ============================================================================

# 執行程式（使用預設資料庫）
run: $(TARGET)
	@echo "$(YELLOW)啟動 C-SQL...$(NC)"
	./$(TARGET) mydb.db

# 執行程式（使用新的測試資料庫）
run-test: $(TARGET)
	@rm -f test.db
	@echo "$(YELLOW)啟動 C-SQL (test.db)...$(NC)"
	./$(TARGET) test.db

# ============================================================================
# 範例與示範
# ============================================================================

# 執行基本範例
demo: $(TARGET)
	@echo "$(YELLOW)執行示範...$(NC)"
	@rm -f demo.db
	@echo "$(GREEN)=== C-SQL 功能示範 ===$(NC)"
	@echo "insert 1 alice alice@example.com\ninsert 2 bob bob@example.com\ninsert 3 charlie charlie@example.com\nselect\nselect where id > 1\nupdate - newemail@example.com where id = 2\nselect\ndelete where id = 3\nselect\n.exit" | ./$(TARGET) demo.db
	@rm -f demo.db
	@echo "$(GREEN)✓ 示範完成！$(NC)"

# ============================================================================
# 說明
# ============================================================================

# 顯示幫助訊息
help:
	@echo "$(GREEN)C-SQL Makefile 使用說明$(NC)"
	@echo ""
	@echo "編譯相關："
	@echo "  make          - 編譯 Release 版本（預設）"
	@echo "  make all      - 同 make"
	@echo "  make debug    - 編譯 Debug 版本"
	@echo "  make rebuild  - 重新編譯"
	@echo ""
	@echo "測試相關："
	@echo "  make test        - 執行完整測試套件"
	@echo "  make test-where  - 執行 WHERE 子句測試"
	@echo "  make test-quick  - 執行快速測試"
	@echo "  make rebuild-test - 重新編譯並測試"
	@echo ""
	@echo "執行相關："
	@echo "  make run      - 執行程式（mydb.db）"
	@echo "  make run-test - 執行程式（test.db）"
	@echo "  make demo     - 執行功能示範"
	@echo ""
	@echo "清理相關："
	@echo "  make clean     - 清理編譯產物"
	@echo "  make clean-all - 清理所有檔案（包括資料庫）"
	@echo ""
	@echo "開發輔助："
	@echo "  make check  - 檢查程式碼（需安裝 cppcheck）"
	@echo "  make format - 格式化程式碼（需安裝 clang-format）"
	@echo ""
	@echo "其他："
	@echo "  make help - 顯示此幫助訊息"

# ============================================================================
# 特殊目標
# ============================================================================

# 顯示版本資訊
version:
	@echo "C-SQL 版本資訊："
	@echo "  編譯器: $(CC)"
	@echo "  編譯選項: $(CFLAGS)"
	@echo "  最後更新: 2025-10-17"

# 顯示編譯資訊
info:
	@echo "目標檔案: $(TARGET)"
	@echo "原始檔案: $(SOURCE)"
	@echo "編譯器: $(CC)"
	@echo "編譯選項 (Release): $(CFLAGS)"
	@echo "編譯選項 (Debug): $(CFLAGS_DEBUG)"

