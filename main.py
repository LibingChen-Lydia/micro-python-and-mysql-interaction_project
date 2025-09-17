import argparse
from getpass import getpass
from typing import List, Tuple
from mysql_helper import MySqlHelper

TABLE_SQL = """
CREATE TABLE IF NOT EXISTS test_users (
    id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

# 演示用样例数据
SAMPLE_USERS: List[Tuple[str, str]] = [
    ("Alice Smith", "alice@example.com"),
    ("Bob Johnson", "bob@example.com"),
    ("Charlie Brown", "charlie@example.com"),
]

def cmd_init(db: MySqlHelper):
    """创建演示表"""
    db.execute_non_query(TABLE_SQL)
    print("✅ Table `test_users` is ready.")

def cmd_seed(db: MySqlHelper):
    """批量插入示例数据（去重插入）"""
    db.execute_many(
        "INSERT IGNORE INTO test_users (name, email) VALUES (%s, %s)",
        SAMPLE_USERS
    )
    print("✅ Seeded sample users (INSERT IGNORE).")

def cmd_insert(db: MySqlHelper, name: str, email: str):
    """插入单条记录"""
    # 先校验，防止空字符串或仅空格
    if not email.strip():
        raise ValueError("Email cannot be empty")

    rows = db.execute_non_query(
        "INSERT INTO test_users (name, email) VALUES (%s, %s)",
        (name, email)
    )
    print(f"✅ Inserted rows: {rows}")

def cmd_list(db: MySqlHelper, order: str, limit: int):
    """查询并按需排序"""
    order_clause = "ORDER BY id ASC" if order == "id" else "ORDER BY created_at DESC"
    sql = f"SELECT id, name, email, created_at FROM test_users {order_clause} LIMIT %s"
    rows = db.execute_query(sql, (limit,))
    if not rows:
        print("No data.")
        return
    for r in rows:
        print(f"{r['id']:>3} | {r['name']:<20} | {r['email']:<25} | {r['created_at']}")

def cmd_update_name(db: MySqlHelper, user_id: int, new_name: str):
    """通过主键安全更新姓名"""
    rows = db.execute_non_query(
        "UPDATE test_users SET name = %s WHERE id = %s",
        (new_name, user_id)
    )
    print(f"✅ Updated rows: {rows}")

def cmd_update_email(db: MySqlHelper, user_id: int, new_email: str):
    """通过主键安全更新邮箱"""
    rows = db.execute_non_query(
        "UPDATE test_users SET email = %s WHERE id = %s",
        (new_email, user_id)
    )
    print(f"✅ Updated rows: {rows}")

def cmd_delete(db: MySqlHelper, user_id: int):
    """通过主键删除"""
    rows = db.execute_non_query(
        "DELETE FROM test_users WHERE id = %s",
        (user_id,)
    )
    print(f"Deleted rows: {rows}")

def cmd_count(db: MySqlHelper):
    """统计行数"""
    cnt = db.execute_query("SELECT COUNT(*) AS c FROM test_users")
    print(f"Row count: {cnt[0]['c'] if cnt else 0}")

def cmd_dedupe(db: MySqlHelper):
    """
    删除重复行：按 email 视为唯一，保留每个 email 的最小id 其余删掉
    """
    # 先查出重复的 email 对应要删除的 id 集合
    to_delete = db.execute_query("""
        SELECT DISTINCT t1.id
        FROM test_users t1
        JOIN test_users t2
          ON t1.email = t2.email AND t1.id > t2.id
    """)
    if not to_delete:
        print("✅ No duplicates.")
        return
    ids = [row["id"] for row in to_delete]
    # 分批删除，避免 IN 列表过大
    CHUNK = 500
    total = 0
    for i in range(0, len(ids), CHUNK):
        batch = ids[i:i+CHUNK]
        placeholders = ",".join(["%s"] * len(batch))
        sql = f"DELETE FROM test_users WHERE id IN ({placeholders})"
        total += db.execute_non_query(sql, tuple(batch))
    print(f"Removed duplicates: {total}")

def cmd_tx_demo(db: MySqlHelper):
    """事务示例：两条操作要么都成功，要么都回滚"""
    try:
        with db._get_cursor() as c:  # 用你的 helper 的事务封装也可以，这里展示失败回滚
            # 手动开始事务：关闭自动提交
            db._get_connection().autocommit(False)
        # 用你已有的上下文管理器会更优雅；此处用 execute_non_query 触发回滚/提交
        try:
            # 假设先插入一条
            db.execute_non_query(
                "INSERT INTO test_users (name, email) VALUES (%s, %s)",
                ("TxUser", "tx@example.com"),
            )
            # 再执行一个会失败的语句（违反唯一约束）
            db.execute_non_query(
                "INSERT INTO test_users (name, email) VALUES (%s, %s)",
                ("TxUserDup", "tx@example.com"),  # duplicate email
            )
            # 成功则提交
            db._get_connection().commit()
        except Exception as e:
            db._get_connection().rollback()
            print(f"❌ Transaction rolled back: {e}")
        finally:
            db._get_connection().autocommit(True)
    except Exception as e:
        print(f"❌ TX demo error: {e}")

def cmd_drop(db: MySqlHelper):
    """删除演示表（谨慎）"""
    db.execute_non_query("DROP TABLE IF EXISTS test_users")
    print("🧯 Dropped table `test_users`.")


def cmd_reindex(db: MySqlHelper):
    """重排 ID，让 id 连续"""
    db.execute_non_query("SET @count = 0;")
    db.execute_non_query("UPDATE test_users SET id = (@count := @count + 1) ORDER BY id;")
    db.execute_non_query("ALTER TABLE test_users AUTO_INCREMENT = 1;")
    print("✅ Reindexed table, IDs are now continuous.")

def build_parser():
    p = argparse.ArgumentParser(description="MySQL demo with MySqlHelper")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=3306)
    p.add_argument("--user", default="root")
    p.add_argument("--password", default=None, help="If omitted, you will be prompted.")
    p.add_argument("--database", "-d", required=True, help="Target database name")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("init", help="Create demo table")
    sub.add_parser("seed", help="Insert sample rows (INSERT IGNORE)")
    sp_ins = sub.add_parser("insert", help="Insert one row")
    sp_ins.add_argument("--name", required=True)
    sp_ins.add_argument("--email", required=True)

    sp_list = sub.add_parser("list", help="List rows")
    sp_list.add_argument("--order", choices=["id", "time"], default="time",
                         help="id: by id asc, time: by created_at desc")
    sp_list.add_argument("--limit", type=int, default=50)

    sp_upn = sub.add_parser("update-name", help="Update name by id")
    sp_upn.add_argument("--id", type=int, required=True)
    sp_upn.add_argument("--name", required=True)

    sp_upe = sub.add_parser("update-email", help="Update email by id")
    sp_upe.add_argument("--id", type=int, required=True)
    sp_upe.add_argument("--email", required=True)

    sp_del = sub.add_parser("delete", help="Delete by id")
    sp_del.add_argument("--id", type=int, required=True)

    sub.add_parser("count", help="Count rows")
    sub.add_parser("dedupe", help="Remove duplicate emails (keep smallest id)")
    sub.add_parser("tx-demo", help="Transaction demo with rollback on error")
    sub.add_parser("drop", help="Drop demo table")

    sub.add_parser("reindex", help="Re-sequence IDs to be continuous")
    sp_init_table = sub.add_parser("init-table", help="Create table from SQL file")
    sp_init_table.add_argument("--sql-file", required=True, help="Path to .sql file")
    sp_init_table.add_argument("--table-name", help="Optional table name for existence check")

    return p

def main():
    parser = build_parser()
    args = parser.parse_args()
    password = args.password or getpass("MySQL password: ")

    # 建立连接（把 charset/超时等传给 kwargs 也可以）
    db = MySqlHelper(
        host=args.host,
        port=args.port,
        user=args.user,
        password=password,
        database=args.database,
        charset="utf8mb4"  # 透传给你的 helper 的 **kwargs
    )

    try:
        if args.cmd == "init":
            cmd_init(db)
        elif args.cmd == "seed":
            cmd_seed(db)
        elif args.cmd == "insert":
            cmd_insert(db, args.name, args.email)
        elif args.cmd == "list":
            cmd_list(db, args.order, args.limit)
        elif args.cmd == "update-name":
            cmd_update_name(db, args.id, args.name)
        elif args.cmd == "update-email":
            cmd_update_email(db, args.id, args.email)
        elif args.cmd == "delete":
            cmd_delete(db, args.id)
        elif args.cmd == "count":
            cmd_count(db)
        elif args.cmd == "dedupe":
            cmd_dedupe(db)
        elif args.cmd == "tx-demo":
            cmd_tx_demo(db)
        elif args.cmd == "drop":
            cmd_drop(db)
        elif args.cmd == "reindex":
            cmd_reindex(db)
        elif args.cmd == "init-table":
            with open(args.sql_file, encoding="utf-8") as f:
                sql_text = f.read()
            db.ensure_table(sql_text, table_name=args.table_name)
            print(f"✅ Table from {args.sql_file} ready.")
        else:
            parser.print_help()
    finally:
        db.close()

if __name__ == "__main__":
    main()
