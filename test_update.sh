#!/bin/bash

# UPDATE 功能測試腳本
# 此腳本演示 UPDATE 語句的各種使用情況

echo "=========================================="
echo "  C-SQL UPDATE 功能測試"
echo "=========================================="
echo ""

# 清理舊的測試資料庫
rm -f test_update.db

echo "測試 1: 基本 UPDATE 操作"
echo "----------------------------------------"
./c_sql test_update.db << EOF
insert 1 alice alice@example.com
insert 2 bob bob@example.com
insert 3 charlie charlie@example.com
select
update 2 bob_updated bob_new@example.com
select
.exit
EOF

echo ""
echo "測試 2: UPDATE 不存在的記錄"
echo "----------------------------------------"
./c_sql test_update.db << EOF
update 999 nonexist nonexist@example.com
.exit
EOF

echo ""
echo "測試 3: 資料持久化測試"
echo "----------------------------------------"
echo "關閉後重新開啟資料庫，檢查更新是否已保存..."
./c_sql test_update.db << EOF
select
.exit
EOF

echo ""
echo "測試 4: 多次 UPDATE 同一筆資料"
echo "----------------------------------------"
./c_sql test_update.db << EOF
update 1 alice_v2 alice_v2@example.com
update 1 alice_v3 alice_v3@example.com
select
.exit
EOF

echo ""
echo "測試 5: UPDATE 所有資料"
echo "----------------------------------------"
./c_sql test_update.db << EOF
update 1 user1 user1@example.com
update 2 user2 user2@example.com
update 3 user3 user3@example.com
select
.exit
EOF

echo ""
echo "=========================================="
echo "  測試完成！"
echo "=========================================="

# 清理測試資料庫
rm -f test_update.db

