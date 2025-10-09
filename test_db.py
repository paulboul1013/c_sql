import subprocess
from pathlib import Path


def run_test(commands, db_filename="mydb.db", reset_db=True):
    binary_path = Path(__file__).resolve().with_name("main")
    if not binary_path.exists():
        raise FileNotFoundError(f"未找到執行檔: {binary_path}")

    db_path = binary_path.with_name(db_filename)
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


if __name__ == "__main__":
    def print_result(title, stdout, stderr, code):
        print(f"=== {title}標準輸出 ===")
        print(stdout, end="")

        if stderr:
            print(f"=== {title}標準錯誤 ===")
            print(stderr, end="")

        print(f"=== {title}程式結束碼: {code} ===")


    first_session_commands = [
        ".constants",
        ".btree",
        "insert 1 user1 user1@example.com",
        "insert 2 user2 user2@example.com",
        "insert 3 user3 user3@example.com",
        "insert 4 user4 user4@example.com",
        "insert 5 user5 user5@example.com",
        "insert 6 user6 user6@example.com",
        "insert 7 user7 user7@example.com",
        "insert 8 user8 user8@example.com",
        "insert 9 user9 user9@example.com",
        "insert 10 user10 user10@example.com",
        "insert 11 user11 user11@example.com",
        "insert 12 user12 user12@example.com",
        "insert 13 user13 user13@example.com",
        "insert 14 user14 user14@example.com",
        "insert 15 user15 user15@example.com",
        "insert 16 user16 user16@example.com",
        "insert 17 user17 user17@example.com",
        "insert 18 user18 user18@example.com",
        "insert 19 user19 user19@example.com",
        "insert 20 user20 user20@example.com",
        "insert 21 user21 user21@example.com",
        "insert 22 user22 user22@example.com",
        "insert 23 user23 user23@example.com",
        "insert 24 user24 user24@example.com",
        "insert 25 user25 user25@example.com",
        "insert 26 user26 user26@example.com",
        "insert 27 user27 user27@example.com",
        "insert 28 user28 user28@example.com",
        "insert 29 user29 user29@example.com",
        "insert 30 user30 user30@example.com",
        "insert 31 user31 user31@example.com",
        "insert 32 user32 user32@example.com",
        "insert 33 user33 user33@example.com",
        "insert 34 user34 user34@example.com",
        "insert 35 user35 user35@example.com",
        "insert 36 user36 user36@example.com",
        "insert 37 user37 user37@example.com",
        "insert 38 user38 user38@example.com",
        "insert 39 user39 user39@example.com",
        "insert 40 user40 user40@example.com",
        "insert 41 user41 user41@example.com",
        "insert 42 user42 user42@example.com",
        "insert 43 user43 user43@example.com",
        "insert 44 user44 user44@example.com",
        "insert 45 user45 user45@example.com",
        "insert 46 user46 user46@example.com",
        "insert 47 user47 user47@example.com",
        "insert 48 user48 user48@example.com",
        "insert 49 user49 user49@example.com",
        "insert 50 user50 user50@example.com",
        "insert 51 user51 user51@example.com",
        "insert 52 user52 user52@example.com",
        "insert 53 user53 user53@example.com",
        "insert 54 user54 user54@example.com",
        "insert 55 user55 user55@example.com",
        "insert 56 user56 user56@example.com",
        "insert 57 user57 user57@example.com",
        "insert 58 user58 user58@example.com",
        "insert 59 user59 user59@example.com",
        "insert 60 user60 user60@example.com",
        "insert 61 user61 user61@example.com",
        "insert 62 user62 user62@example.com",
        "insert 63 user63 user63@example.com",
        "insert 64 user64 user64@example.com",
        "insert 65 user65 user65@example.com",
        "insert 66 user66 user66@example.com",
        "insert 67 user67 user67@example.com",
        "insert 68 user68 user68@example.com",
        "insert 69 user69 user69@example.com",
        "insert 70 user70 user70@example.com",
        "insert 71 user71 user71@example.com",
        "insert 72 user72 user72@example.com",
        "insert 73 user73 user73@example.com",
        "insert 74 user74 user74@example.com",
        "insert 75 user75 user75@example.com",
        "insert 76 user76 user76@example.com",
        "insert 77 user77 user77@example.com",
        "insert 78 user78 user78@example.com",
        "insert 79 user79 user79@example.com",
        "insert 80 user80 user80@example.com",
        "insert 81 user81 user81@example.com",
        "insert 82 user82 user82@example.com",
        "insert 83 user83 user83@example.com",
        "insert 84 user84 user84@example.com",
        "insert 85 user85 user85@example.com",
        "insert 86 user86 user86@example.com",
        ".btree",
        "select",
        ".exit",
    ]

    stdout1, stderr1, code1 = run_test(first_session_commands)
    print_result("第一次", stdout1, stderr1, code1)

    second_session_commands = [
        ".constants",
        ".btree",
        "select",
        ".exit",
    ]

    stdout2, stderr2, code2 = run_test(second_session_commands, reset_db=False)
    print_result("第二次", stdout2, stderr2, code2)

