import subprocess
from pathlib import Path
import atexit


# 用於追蹤測試過程中創建的所有資料庫檔案
created_db_files = set()

def run_test(commands, db_filename="mydb.db", reset_db=True):
    binary_path = Path(__file__).resolve().with_name("main")
    if not binary_path.exists():
        raise FileNotFoundError(f"未找到執行檔: {binary_path}")

    db_path = binary_path.with_name(db_filename)
    
    # 記錄創建的資料庫檔案
    created_db_files.add(db_path)
    
    if reset_db and db_path.exists():
        db_path.unlink()

    joined = "\n".join(commands) + "\n"
    result = subprocess.run(
        [str(binary_path), str(db_path)],
        input=joined,
        text=True,
        capture_output=True,
        check=False,
    )
    return result.stdout, result.stderr, result.returncode


def print_result(title, stdout, stderr, code):
    print(f"=== {title}標準輸出 ===")
    print(stdout, end="")

    if stderr:
        print(f"=== {title}標準錯誤 ===")
        print(stderr, end="")

    print(f"=== {title}程式結束碼: {code} ===")


def cleanup_db_files():
    """清理測試過程中創建的所有資料庫檔案"""
    print("\n" + "="*50)
    print("清理測試資料庫檔案...")
    print("="*50)
    
    for db_path in created_db_files:
        if db_path.exists():
            try:
                db_path.unlink()
                print(f"已刪除: {db_path.name}")
            except Exception as e:
                print(f"無法刪除 {db_path.name}: {e}")
    
    print(f"共清理 {len(created_db_files)} 個資料庫檔案")
    print("="*50)


def test_basic_operations():
    """測試基本 CRUD 操作"""
    print("\n" + "="*50)
    print("測試 1: 基本 CRUD 操作")
    print("="*50)
    
    commands = [
        "insert 1 alice alice@example.com",
        "insert 2 bob bob@example.com", 
        "insert 3 charlie charlie@example.com",
        "select",
        "update 2 bobby bobby@example.com",
        "select",
        "delete 3",
        "select",
        ".exit"
    ]
    
    stdout, stderr, code = run_test(commands)
    print_result("基本操作", stdout, stderr, code)


def test_node_splitting():
    """測試節點分裂功能"""
    print("\n" + "="*50)
    print("測試 2: 節點分裂功能")
    print("="*50)
    
    # 插入 15 筆資料以觸發節點分裂
    commands = [".constants", ".btree"]
    for i in range(1, 16):
        commands.append(f"insert {i} user{i} user{i}@example.com")
    commands.extend([".btree", "select", ".exit"])
    
    stdout, stderr, code = run_test(commands)
    print_result("節點分裂", stdout, stderr, code)


def test_node_merging():
    """測試節點合併功能"""
    print("\n" + "="*50)
    print("測試 3: 節點合併功能")
    print("="*50)
    
    # 先插入大量資料
    commands = []
    for i in range(1, 80):
        commands.append(f"insert {i} user{i} user{i}@example.com")
    commands.extend([".btree", "select"])
    
    stdout1, stderr1, code1 = run_test(commands)
    print_result("插入資料", stdout1, stderr1, code1)
    
    # 然後刪除大量資料以觸發合併
    delete_commands = []
    for i in range(11, 50):
        delete_commands.append(f"delete {i}")
    delete_commands.extend([".btree", "select", ".exit"])
    
    stdout2, stderr2, code2 = run_test(delete_commands, db_filename="mydb.db", reset_db=False)
    print_result("刪除觸發合併", stdout2, stderr2, code2)


def test_error_handling():
    """測試錯誤處理"""
    print("\n" + "="*50)
    print("測試 4: 錯誤處理")
    print("="*50)
    
    commands = [
        "insert 1 test test@example.com",
        "insert 1 duplicate duplicate@example.com",  # 重複 ID
        "update 999 notfound notfound@example.com",  # 不存在的 ID
        "delete 999",  # 不存在的 ID
        "select",
        ".exit"
    ]
    
    stdout, stderr, code = run_test(commands)
    print_result("錯誤處理", stdout, stderr, code)


def test_persistence():
    """測試資料持久化"""
    print("\n" + "="*50)
    print("測試 5: 資料持久化")
    print("="*50)
    
    # 第一次會話：插入資料
    session1_commands = [
        "insert 1 persistent1 user1@example.com",
        "insert 2 persistent2 user2@example.com",
        "insert 3 persistent3 user3@example.com",
        "select",
        ".exit"
    ]
    
    stdout1, stderr1, code1 = run_test(session1_commands, db_filename="persistence_test.db")
    print_result("第一次會話", stdout1, stderr1, code1)
    
    # 第二次會話：驗證資料是否持久化
    session2_commands = [
        "select",
        "insert 4 persistent4 user4@example.com",
        "select",
        ".exit"
    ]
    
    stdout2, stderr2, code2 = run_test(session2_commands, db_filename="persistence_test.db", reset_db=False)
    print_result("第二次會話", stdout2, stderr2, code2)


def test_large_dataset():
    """測試大量資料處理"""
    print("\n" + "="*50)
    print("測試 6: 大量資料處理")
    print("="*50)
    
    commands = [".constants"]
    
    # 插入 100 筆資料
    for i in range(1, 101):
        commands.append(f"insert {i} user{i} user{i}@example.com")
    
    commands.extend([".btree", "select", ".exit"])
    
    stdout, stderr, code = run_test(commands, db_filename="large_dataset.db")
    print_result("大量資料", stdout, stderr, code)


def test_where_clause():
    """測試 WHERE 子句功能"""
    print("\n" + "="*50)
    print("測試 7: WHERE 子句功能")
    print("="*50)
    
    commands = [
        # 插入測試資料
        "insert 1 alice alice@example.com",
        "insert 2 bob bob@example.com",
        "insert 3 charlie charlie@example.com",
        "insert 4 david david@example.com",
        "insert 5 eve eve@example.com",
        "insert 6 frank frank@example.com",
        "insert 7 grace grace@example.com",
        
        # 測試 SELECT WHERE - 等於運算符
        "select where id = 3",
        "select where username = bob",
        "select where email = eve@example.com",
        
        # 測試 SELECT WHERE - 不等於運算符
        "select where id != 3",
        "select where username != alice",
        
        # 測試 SELECT WHERE - 大於/小於運算符
        "select where id > 4",
        "select where id < 3",
        "select where id >= 5",
        "select where id <= 2",
        
        # 測試 UPDATE WHERE - 使用 id 條件
        "update new_name new@example.com where id = 1",
        "select where id = 1",
        
        # 測試 UPDATE WHERE - 部分欄位更新
        "update - updated_bob@example.com where id = 2",
        "select where id = 2",
        
        "update updated_charlie - where id = 3",
        "select where id = 3",
        
        # 測試 UPDATE WHERE - 使用 username 條件
        "update new_david new_david@example.com where username = david",
        "select where username = new_david",
        
        # 測試 UPDATE WHERE - 批次更新
        "update batch_user - where id > 5",
        "select where id > 5",
        
        # 測試 DELETE WHERE - 單筆刪除
        "delete where id = 7",
        "select",
        
        # 測試 DELETE WHERE - 使用 username 條件
        "delete where username = eve",
        "select",
        
        # 測試 DELETE WHERE - 批次刪除
        "delete where id > 4",
        "select",
        
        ".exit"
    ]
    
    stdout, stderr, code = run_test(commands, db_filename="where_test.db")
    print_result("WHERE 子句", stdout, stderr, code)


def test_where_edge_cases():
    """測試 WHERE 子句的邊界情況"""
    print("\n" + "="*50)
    print("測試 8: WHERE 子句邊界情況")
    print("="*50)
    
    commands = [
        # 插入測試資料
        "insert 1 user1 user1@example.com",
        "insert 2 user2 user2@example.com",
        "insert 3 user3 user3@example.com",
        
        # 測試不存在的資料
        "select where id = 999",
        "update nonexist - where id = 999",
        "delete where id = 999",
        
        # 測試空結果集
        "select where id > 100",
        "update nothing - where username = nonexistent",
        
        # 測試字串比較
        "select where username > user1",
        "select where username < user3",
        
        # 確認資料未被錯誤修改
        "select",
        
        ".exit"
    ]
    
    stdout, stderr, code = run_test(commands, db_filename="where_edge_test.db")
    print_result("WHERE 邊界情況", stdout, stderr, code)


def test_where_performance():
    """測試 WHERE 子句在大量資料下的效能"""
    print("\n" + "="*50)
    print("測試 9: WHERE 子句效能測試")
    print("="*50)
    
    commands = []
    
    # 插入 50 筆資料
    for i in range(1, 51):
        commands.append(f"insert {i} user{i} user{i}@example.com")
    
    # 測試不同的 WHERE 查詢
    commands.extend([
        "select where id = 25",  # 精確查找
        "select where id > 40",   # 範圍查詢
        "select where id < 10",   # 範圍查詢
        "select where username = user15",  # 字串精確查找
        
        # 批次更新
        "update batch_update - where id > 45",
        "select where id > 45",
        
        # 批次刪除
        "delete where id > 45",
        "select where id > 40",
        
        ".exit"
    ])
    
    stdout, stderr, code = run_test(commands, db_filename="where_perf_test.db")
    print_result("WHERE 效能", stdout, stderr, code)


def test_where_complex_conditions():
    """測試 WHERE 子句的複雜條件（AND、OR）"""
    print("\n" + "="*50)
    print("測試 10: WHERE 子句複雜條件（AND、OR）")
    print("="*50)
    
    commands = [
        # 插入測試資料
        "insert 1 alice alice@example.com",
        "insert 2 bob bob@example.com",
        "insert 3 charlie charlie@example.com",
        "insert 4 dave dave@example.com",
        "insert 5 eve eve@example.com",
        "insert 10 admin admin@example.com",
        "insert 15 test test@example.com",
        "insert 20 user user@example.com",
        
        # 測試 AND 條件
        "select where id > 2 AND id < 5",
        "select where id >= 1 AND id <= 3",
        "select where username = alice AND id = 1",
        
        # 測試 OR 條件
        "select where id = 1 OR id = 10",
        "select where username = alice OR username = admin",
        "select where id < 3 OR id > 15",
        
        # 測試混合條件（從左到右評估）
        "select where id > 1 AND id < 5 OR id = 10",
        "select where username = alice OR id > 10 AND id < 20",
        
        # 測試多個 AND 條件
        "select where id > 0 AND id < 10 AND id != 5",
        "select where id > 2 AND id < 8 AND username != eve",
        
        # 測試多個 OR 條件
        "select where id = 1 OR id = 5 OR id = 10",
        "select where username = alice OR username = bob OR username = charlie",
        
        # 測試 UPDATE 使用複雜條件
        "update updated_name - where id = 2 OR id = 3",
        "select where id >= 2 AND id <= 3",
        
        "update batch_user - where id > 10 AND id < 20",
        "select where id > 10 AND id < 20",
        
        # 測試 DELETE 使用複雜條件
        "delete where id > 15 AND id < 25",
        "select",
        
        "delete where username = updated_name OR id = 10",
        "select",
        
        # 測試複雜的字串條件
        "select where username = alice OR username = admin AND id > 0",
        
        # 測試不等於運算符與邏輯運算符組合
        "select where id != 1 AND id != 5",
        "select where username != alice OR id = 1",
        
        ".exit"
    ]
    
    stdout, stderr, code = run_test(commands, db_filename="where_complex_test.db")
    print_result("WHERE 複雜條件", stdout, stderr, code)


def test_where_parenthesis():
    """測試 WHERE 子句的括號優先級"""
    print("\n" + "="*50)
    print("測試 11: WHERE 子句括號優先級")
    print("="*50)
    
    commands = [
        # 插入測試資料
        "insert 1 alice alice@example.com",
        "insert 2 bob bob@example.com",
        "insert 3 charlie charlie@example.com",
        "insert 4 diana diana@example.com",
        "insert 5 eve eve@example.com",
        "insert 10 admin admin@example.com",
        "insert 15 root root@example.com",
        "insert 20 user user@example.com",
        
        # 測試基本查詢（無括號）
        "select where id < 5 AND username = alice",
        "select where id < 5 OR id > 10",
        
        # 測試括號改變優先級：(A OR B) AND C
        "select where (id < 5 OR id > 10) AND username != user",
        
        # 測試 A OR (B AND C)
        "select where id = 1 OR (id > 10 AND id < 20)",
        
        # 測試多個括號組合：(A OR B) AND (C OR D)
        "select where (id < 3 OR id > 15) AND (username = alice OR username = root)",
        
        # 測試嵌套括號：((A OR B) AND C) OR D
        "select where ((id < 3 OR id > 15) AND username = alice) OR username = bob",
        
        # 測試複雜嵌套：(A AND B) OR (C AND D)
        "select where (id < 5 AND username != bob) OR (id > 10 AND username != user)",
        
        # 測試三層嵌套：((A OR B) AND C) OR (D AND E)
        "select where ((id < 3 OR id = 5) AND username = alice) OR (id > 15 AND username = root)",
        
        # 測試 UPDATE 語句中的括號
        "update test_user test@example.com where (id < 3 OR id > 10) AND username != user",
        "select where username = test_user",
        
        # 測試 UPDATE 使用複雜括號條件
        "update updated - where (id = 5 OR id = 10) AND username != updated",
        "select where username = updated",
        
        # 測試 DELETE 語句中的括號
        "delete where (id = 3 OR id = 4) AND username != test_user",
        "select",
        
        # 測試 DELETE 使用嵌套括號
        "delete where ((id < 3 OR id > 15) AND username = test_user) OR username = eve",
        "select",
        
        # 測試括號中的所有運算符
        "select where (id > 5 AND id < 15) OR (id >= 1 AND id <= 2)",
        "select where (id != 10 AND id != 15) OR (username = alice AND username = bob)",
        
        # 測試邊界情況：單一括號
        "select where (id = 10)",
        "select where (username = admin)",
        
        # 測試邊界情況：全部都在括號中
        "select where ((id > 0))",
        
        ".exit"
    ]
    
    stdout, stderr, code = run_test(commands, db_filename="where_parenthesis_test.db")
    print_result("WHERE 括號優先級", stdout, stderr, code)


def test_acid_transactions():
    """測試 ACID 交易功能"""
    print("\n" + "="*50)
    print("測試 12: ACID 交易功能")
    print("="*50)
    
    commands = [
        # 插入初始資料
        "insert 1 user1 user1@example.com",
        "insert 2 user2 user2@example.com",
        "insert 3 user3 user3@example.com",
        "select",
        
        # 測試 1: 基本 COMMIT
        "BEGIN",
        "insert 4 user4 user4@example.com",
        "insert 5 user5 user5@example.com",
        "COMMIT",
        "select",
        
        # 測試 2: 基本 ROLLBACK
        "BEGIN",
        "insert 6 user6 user6@example.com",
        "insert 7 user7 user7@example.com",
        "ROLLBACK",
        "select",  # 應該不包含 user6 和 user7
        
        # 測試 3: 交易中的 UPDATE 然後 COMMIT
        "BEGIN",
        "update 1 updated1 updated1@example.com",
        "update 2 updated2 updated2@example.com",
        "COMMIT",
        "select",
        
        # 測試 4: 交易中的 UPDATE 然後 ROLLBACK
        "BEGIN",
        "update 1 rollback_user rollback@example.com",
        "ROLLBACK",
        "select where id = 1",  # 應該還是 updated1
        
        # 測試 5: 交易中的 DELETE 然後 COMMIT
        "BEGIN",
        "delete where id = 3",
        "COMMIT",
        "select",
        
        # 測試 6: 交易中的 DELETE 然後 ROLLBACK
        "BEGIN",
        "delete where id = 2",
        "ROLLBACK",
        "select",  # id = 2 的資料應該還在
        
        # 測試 7: 混合操作 COMMIT
        "BEGIN",
        "insert 10 user10 user10@example.com",
        "update 10 updated10 updated10@example.com",
        "insert 20 user20 user20@example.com",
        "COMMIT",
        "select",
        
        # 測試 8: 混合操作 ROLLBACK
        "BEGIN",
        "insert 30 user30 user30@example.com",
        "update 1 should_rollback rollback@example.com",
        "delete where id = 10",
        "ROLLBACK",
        "select",  # 所有操作都應該被回滾
        
        # 測試 9: 交易中查看資料（應該看到未提交的修改）
        "BEGIN",
        "insert 100 temp100 temp100@example.com",
        "update 1 temp_update temp@example.com",
        "select",  # 應該看到交易中的修改
        "ROLLBACK",
        "select",  # 修改應該被回滾
        
        # 測試 10: WHERE 子句在交易中
        "BEGIN",
        "update batch_txn - where id > 4",
        "select where id > 4",  # 應該看到修改
        "ROLLBACK",
        "select where id > 4",  # 修改應該被回滾
        
        # 測試 11: 複雜條件在交易中
        "BEGIN",
        "update test_user test@example.com where (id < 3 OR id > 15) AND id != 1",
        "select where username = test_user",
        "COMMIT",
        "select where username = test_user",  # 修改應該被保留
        
        ".exit"
    ]
    
    stdout, stderr, code = run_test(commands, db_filename="acid_test.db")
    print_result("ACID 交易", stdout, stderr, code)


def test_select_range():
    """測試 SELECT 範圍查詢最佳化"""
    print("\n" + "="*50)
    print("測試 13: SELECT 範圍查詢最佳化")
    print("="*50)
    
    commands = []
    
    # 插入大量測試資料（1-50）
    for i in range(1, 51):
        commands.append(f"insert {i} user{i} user{i}@example.com")
    
    # 測試 1: 精確查找（索引查找 - Index Lookup）
    commands.extend([
        "select where id = 25",  # 應該使用 B-tree 索引查找
        "select where id = 1",
        "select where id = 50",
    ])
    
    # 測試 2: 範圍查詢 - 大於（範圍掃描 - Range Scan）
    commands.extend([
        "select where id > 45",  # 應該從 id=46 開始掃描
        "select where id > 40",
        "select where id > 1",
    ])
    
    # 測試 3: 範圍查詢 - 大於等於
    commands.extend([
        "select where id >= 45",  # 應該從 id=45 開始掃描
        "select where id >= 40",
        "select where id >= 1",
    ])
    
    # 測試 4: 範圍查詢 - 小於
    commands.extend([
        "select where id < 5",  # 應該從頭掃描到 id<5
        "select where id < 10",
        "select where id < 50",
    ])
    
    # 測試 5: 範圍查詢 - 小於等於
    commands.extend([
        "select where id <= 5",
        "select where id <= 10",
        "select where id <= 50",
    ])
    
    # 測試 6: 混合條件（應該優先使用索引）
    commands.extend([
        "select where id = 25 AND username = user25",  # 使用索引查找 id，然後檢查 username
        "select where id > 40 AND username != user45",  # 使用範圍掃描，然後過濾
        "select where id < 10 OR id > 40",  # 需要全表掃描
    ])
    
    # 測試 7: 複雜條件中的索引優化
    commands.extend([
        "select where (id = 10 OR id = 20) AND username != user15",
        "select where id > 30 AND id < 40",
        "select where (id < 10 OR id > 40) AND username = user5",
    ])
    
    # 測試 8: UPDATE 中的範圍查詢優化
    commands.extend([
        "update range_test - where id > 45",  # 範圍更新
        "select where id > 45",
        "update precise_test - where id = 25",  # 精確更新
        "select where id = 25",
    ])
    
    # 測試 9: DELETE 中的範圍查詢優化
    commands.extend([
        "delete where id > 48",  # 範圍刪除
        "select where id > 45",
        "delete where id = 47",  # 精確刪除
        "select where id > 45",
    ])
    
    # 測試 10: 字串查詢（應該使用全表掃描）
    commands.extend([
        "select where username = user30",  # 全表掃描
        "select where username != range_test",  # 全表掃描
    ])
    
    # 測試 11: 邊界情況
    commands.extend([
        "select where id = 0",  # 不存在
        "select where id > 100",  # 超出範圍
        "select where id < 0",  # 空結果
        "select where id >= 51",  # 超出範圍
    ])
    
    commands.append(".exit")
    
    stdout, stderr, code = run_test(commands, db_filename="range_test.db")
    print_result("SELECT 範圍查詢", stdout, stderr, code)


def test_complex_operations():
    """測試複雜操作組合"""
    print("\n" + "="*50)
    print("測試 14: 複雜操作組合")
    print("="*50)
    
    commands = [
        # 插入初始資料
        "insert 1 initial1 user1@example.com",
        "insert 2 initial2 user2@example.com",
        "insert 3 initial3 user3@example.com",
        "insert 4 initial4 user4@example.com",
        "insert 5 initial5 user5@example.com",
        
        # 查看初始狀態
        ".btree",
        "select",
        
        # 更新資料
        "update 2 updated2 updated2@example.com",
        "update 4 updated4 updated4@example.com",
        
        # 刪除部分資料
        "delete 1",
        "delete 3",
        
        # 插入新資料
        "insert 6 new6 user6@example.com",
        "insert 7 new7 user7@example.com",
        
        # 查看最終狀態
        ".btree",
        "select",
        ".exit"
    ]
    
    stdout, stderr, code = run_test(commands)
    print_result("複雜操作", stdout, stderr, code)


def test_statistics_basic():
    """測試統計資訊基本功能"""
    print("\n" + "="*50)
    print("測試 15: 統計資訊基本功能")
    print("="*50)
    
    commands = [
        # 測試 1: 空表的統計資訊
        ".stats",
        
        # 插入資料
        "insert 1 user1 user1@example.com",
        "insert 2 user2 user2@example.com",
        "insert 3 user3 user3@example.com",
        
        # 測試 2: 查看統計資訊（自動更新後的）
        ".stats",
        
        # 測試 3: 執行 ANALYZE 命令
        "ANALYZE",
        
        # 測試 4: 再次查看統計資訊
        ".stats",
        
        # 測試 5: 使用 .analyze 命令
        ".analyze",
        
        # 測試 6: 再次查看統計資訊
        ".stats",
        
        ".exit"
    ]
    
    stdout, stderr, code = run_test(commands, db_filename="stats_basic_test.db")
    print_result("統計資訊基本功能", stdout, stderr, code)


def test_statistics_auto_update():
    """測試統計資訊自動更新"""
    print("\n" + "="*50)
    print("測試 16: 統計資訊自動更新")
    print("="*50)
    
    commands = [
        # 插入初始資料
        "insert 1 user1 user1@example.com",
        "insert 2 user2 user2@example.com",
        "insert 3 user3 user3@example.com",
        "insert 4 user4 user4@example.com",
        "insert 5 user5 user5@example.com",
        
        # 查看初始統計資訊
        ".stats",
        
        # 插入更多資料（應該自動更新統計資訊）
        "insert 6 user6 user6@example.com",
        "insert 7 user7 user7@example.com",
        "insert 8 user8 user8@example.com",
        
        # 查看更新後的統計資訊
        ".stats",
        
        # 刪除資料（應該自動更新統計資訊）
        "delete 1",
        "delete 3",
        "delete 5",
        
        # 查看刪除後的統計資訊
        ".stats",
        
        # 刪除所有資料
        "delete 2",
        "delete 4",
        "delete 6",
        "delete 7",
        "delete 8",
        
        # 查看空表的統計資訊
        ".stats",
        
        ".exit"
    ]
    
    stdout, stderr, code = run_test(commands, db_filename="stats_auto_update_test.db")
    print_result("統計資訊自動更新", stdout, stderr, code)


def test_statistics_large_dataset():
    """測試統計資訊在大量資料下的收集"""
    print("\n" + "="*50)
    print("測試 17: 統計資訊大量資料測試")
    print("="*50)
    
    commands = []
    
    # 插入 50 筆資料
    for i in range(1, 51):
        commands.append(f"insert {i} user{i} user{i}@example.com")
    
    # 執行 ANALYZE
    commands.extend([
        "ANALYZE",
        ".stats",
        
        # 插入更多資料（範圍較大）
        "insert 100 user100 user100@example.com",
        "insert 200 user200 user200@example.com",
        "insert 50 user50_dup user50_dup@example.com",  # 重複 ID（應該失敗）
        
        # 再次執行 ANALYZE
        "ANALYZE",
        ".stats",
        
        # 刪除部分資料
        "delete where id > 40",
        "ANALYZE",
        ".stats",
        
        ".exit"
    ])
    
    stdout, stderr, code = run_test(commands, db_filename="stats_large_test.db")
    print_result("統計資訊大量資料", stdout, stderr, code)


def test_statistics_query_optimization():
    """測試統計資訊用於查詢最佳化"""
    print("\n" + "="*50)
    print("測試 18: 統計資訊查詢最佳化")
    print("="*50)
    
    commands = []
    
    # 插入測試資料（1-30）
    for i in range(1, 31):
        commands.append(f"insert {i} user{i} user{i}@example.com")
    
    # 執行 ANALYZE 收集統計資訊
    commands.extend([
        "ANALYZE",
        ".stats",
        
        # 測試 1: 使用統計資訊的最佳化查詢 - 精確查找
        "select where id = 15",
        
        # 測試 2: 使用統計資訊的最佳化查詢 - 範圍掃描
        "select where id > 25",
        "select where id >= 25",
        "select where id < 10",
        "select where id <= 10",
        
        # 測試 3: 使用統計資訊的最佳化查詢 - 全表掃描（非 ID 欄位）
        "select where username = user15",
        "select where email = user20@example.com",
        
        # 測試 4: 複雜條件（應該會使用統計資訊）
        "select where id = 15 AND username = user15",
        "select where id > 20 AND id < 25",
        
        # 測試 5: 無 WHERE 條件（應該返回所有行）
        "select",
        
        # 插入更多資料以改變統計資訊
        "insert 31 user31 user31@example.com",
        "insert 32 user32 user32@example.com",
        
        # 再次執行 ANALYZE
        "ANALYZE",
        ".stats",
        
        # 測試更新後的統計資訊是否影響查詢
        "select where id > 30",
        
        ".exit"
    ])
    
    stdout, stderr, code = run_test(commands, db_filename="stats_query_opt_test.db")
    print_result("統計資訊查詢最佳化", stdout, stderr, code)


def test_statistics_cardinality():
    """測試統計資訊的基數計算"""
    print("\n" + "="*50)
    print("測試 19: 統計資訊基數計算")
    print("="*50)
    
    commands = [
        # 插入資料（有些重複的 username 和 email）
        "insert 1 alice alice@example.com",
        "insert 2 bob bob@example.com",
        "insert 3 charlie charlie@example.com",
        "insert 4 david david@example.com",
        "insert 5 eve eve@example.com",
        
        # 插入重複的 username
        "insert 6 alice alice2@example.com",  # username 重複
        "insert 7 bob bob2@example.com",      # username 重複
        
        # 插入重複的 email
        "insert 8 frank alice@example.com",    # email 重複
        "insert 9 grace bob@example.com",       # email 重複
        
        # 執行 ANALYZE 查看基數
        "ANALYZE",
        ".stats",
        
        # 插入更多唯一值
        "insert 10 unique1 unique1@example.com",
        "insert 11 unique2 unique2@example.com",
        "insert 12 unique3 unique3@example.com",
        
        # 再次執行 ANALYZE
        "ANALYZE",
        ".stats",
        
        # 刪除部分資料，查看基數變化
        "delete 6",
        "delete 7",
        "ANALYZE",
        ".stats",
        
        ".exit"
    ]
    
    stdout, stderr, code = run_test(commands, db_filename="stats_cardinality_test.db")
    print_result("統計資訊基數計算", stdout, stderr, code)


def test_statistics_id_range():
    """測試統計資訊的 ID 範圍追蹤"""
    print("\n" + "="*50)
    print("測試 20: 統計資訊 ID 範圍追蹤")
    print("="*50)
    
    commands = [
        # 插入非連續的 ID
        "insert 10 user10 user10@example.com",
        "insert 5 user5 user5@example.com",
        "insert 1 user1 user1@example.com",
        "insert 20 user20 user20@example.com",
        "insert 15 user15 user15@example.com",
        
        # 查看統計資訊（應該顯示正確的 ID 範圍）
        "ANALYZE",
        ".stats",
        
        # 插入更小的 ID
        "insert 3 user3 user3@example.com",
        
        # 查看統計資訊（最小值應該更新）
        ".stats",
        
        # 插入更大的 ID
        "insert 100 user100 user100@example.com",
        
        # 查看統計資訊（最大值應該更新）
        ".stats",
        
        # 刪除最小 ID
        "delete 1",
        
        # 查看統計資訊（最小值應該更新）
        ".stats",
        
        # 刪除最大 ID
        "delete 100",
        
        # 查看統計資訊（最大值應該更新）
        ".stats",
        
        # 執行 ANALYZE 確保統計資訊正確
        "ANALYZE",
        ".stats",
        
        ".exit"
    ]
    
    stdout, stderr, code = run_test(commands, db_filename="stats_range_test.db")
    print_result("統計資訊 ID 範圍", stdout, stderr, code)


def test_statistics_edge_cases():
    """測試統計資訊的邊界情況"""
    print("\n" + "="*50)
    print("測試 21: 統計資訊邊界情況")
    print("="*50)
    
    commands = [
        # 測試 1: 空表的統計資訊
        ".stats",
        "ANALYZE",
        ".stats",
        
        # 測試 2: 只有一筆資料
        "insert 1 user1 user1@example.com",
        ".stats",
        "ANALYZE",
        ".stats",
        
        # 測試 3: 刪除所有資料
        "delete 1",
        ".stats",
        "ANALYZE",
        ".stats",
        
        # 測試 4: 插入大量資料後刪除
        "insert 1 user1 user1@example.com",
        "insert 2 user2 user2@example.com",
        "insert 3 user3 user3@example.com",
        "insert 4 user4 user4@example.com",
        "insert 5 user5 user5@example.com",
        
        "ANALYZE",
        ".stats",
        
        # 批次刪除
        "delete 1",
        "delete 2",
        "delete 3",
        "delete 4",
        "delete 5",
        
        ".stats",
        
        # 測試 5: 重複執行 ANALYZE
        "insert 10 user10 user10@example.com",
        "insert 20 user20 user20@example.com",
        
        "ANALYZE",
        ".stats",
        "ANALYZE",
        ".stats",
        "ANALYZE",
        ".stats",
        
        ".exit"
    ]
    
    stdout, stderr, code = run_test(commands, db_filename="stats_edge_test.db")
    print_result("統計資訊邊界情況", stdout, stderr, code)


if __name__ == "__main__":
    # 註冊清理函數，確保程式結束時執行
    atexit.register(cleanup_db_files)
    
    print("C-SQL 資料庫測試套件")
    print("="*50)
    
    # 執行所有測試
    test_basic_operations()
    test_node_splitting()
    test_node_merging()
    test_error_handling()
    test_persistence()
    test_large_dataset()
    test_where_clause()
    test_where_edge_cases()
    test_where_performance()
    test_where_complex_conditions()
    test_where_parenthesis()  # WHERE 括號優先級測試
    test_acid_transactions()  # 新增：ACID 交易測試
    test_select_range()       # 新增：SELECT 範圍查詢測試
    test_complex_operations()
    test_statistics_basic()              # 新增：統計資訊基本功能測試
    test_statistics_auto_update()        # 新增：統計資訊自動更新測試
    test_statistics_large_dataset()      # 新增：統計資訊大量資料測試
    test_statistics_query_optimization() # 新增：統計資訊查詢最佳化測試
    test_statistics_cardinality()        # 新增：統計資訊基數計算測試
    test_statistics_id_range()           # 新增：統計資訊 ID 範圍測試
    test_statistics_edge_cases()        # 新增：統計資訊邊界情況測試
    
    print("\n" + "="*50)
    print("所有測試完成！")
    print("="*50)

