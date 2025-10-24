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


def test_complex_operations():
    """測試複雜操作組合"""
    print("\n" + "="*50)
    print("測試 12: 複雜操作組合")
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
    test_where_parenthesis()  # 新增：WHERE 括號優先級測試
    test_complex_operations()
    
    print("\n" + "="*50)
    print("所有測試完成！")
    print("="*50)

