"""
MySQL Helper Module

This module provides a high-level interface for interacting with MySQL databases
using PyMySQL. It simplifies common database operations and handles connections
safely.
"""

import pymysql # 导入pymysql库：MySQL数据库连接库
from typing import List, Dict, Any, Optional, Union, Tuple
import logging
from contextlib import contextmanager

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MySqlHelper:
    """
    A helper class for MySQL database operations using PyMySQL.
    
    This class provides a simplified interface for common database operations
    including executing queries, non-queries, and batch operations with proper
    connection handling and error management.
    
    Example usage:
        # Initialize the helper
        db = MySqlHelper(
            host='localhost',
            port=3306,
            user='your_username',
            password='your_password',
            database='your_database'
        )
        
        try:
            # Execute a query
            results = db.execute_query("SELECT * FROM users WHERE age > %s", (25,))
            
            # Execute an insert
            rowcount = db.execute_non_query(
                "INSERT INTO users (name, email) VALUES (%s, %s)",
                ('John Doe', 'john@example.com')
            )
            
            # Batch insert
            users = [
                ('Jane Smith', 'jane@example.com'),
                ('Bob Johnson', 'bob@example.com')
            ]
            rowcount = db.execute_many(
                "INSERT INTO users (name, email) VALUES (%s, %s)",
                users
            )
            
        except Exception as e:
            logger.error(f"Database operation failed: {e}")
            raise
            
        finally:
            # Always close the connection
            db.close()
    """
    
    def __init__(
        self,
        host: str,
        user: str,
        password: str,
        database: str,
        port: int = 3306,
        **kwargs
    ) -> None:
        """
        Initialize the MySQL helper with connection parameters.
        
        Args:
            host: Database host address
            user: Database username
            password: Database password
            database: Database name
            port: Database port (default: 3306)
            **kwargs: Additional connection parameters for PyMySQL
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.connection_params = kwargs
        self.connection = None
    
    def _get_connection(self):
        """
        Get a database connection, creating a new one if necessary.
        
        Returns:
            A PyMySQL connection object
            
        Raises:
            pymysql.Error: If connection fails
        """
        if self.connection is None or not self.connection.open:
            try:
                self.connection = pymysql.connect(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    database=self.database,
                    cursorclass=pymysql.cursors.DictCursor,
                    **self.connection_params
                )
                logger.info(f"Connected to MySQL database at {self.host}")
            except pymysql.Error as e:
                logger.error(f"Failed to connect to MySQL: {e}")
                raise
        return self.connection
    
    @contextmanager
    def _get_cursor(self):
        """
        A context manager for handling database cursors.
        
        Yields:
            A database cursor
            
        Example:
            with self._get_cursor() as cursor:
                cursor.execute("SELECT * FROM table")
                result = cursor.fetchall()
        """
        conn = self._get_connection()
        cursor = None
        try:
            cursor = conn.cursor()
            yield cursor
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Database operation failed: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
    
    def create_database_if_not_exists(self, dbname: str, charset: str = "utf8mb4", collate: str = "utf8mb4_general_ci") -> None:
        """
        如果数据库不存在则创建（需要有创建权限）。
        注意：创建库与当前连接已选的 database 无冲突。
        """
        sql = f"CREATE DATABASE IF NOT EXISTS `{dbname}` CHARACTER SET {charset} COLLATE {collate};"
        # 用当前连接直接执行即可
        self.execute_non_query(sql)
    
    def table_exists(self, table_name: str, schema: Optional[str] = None) -> bool:
        """
        判断表是否存在。schema 不传则使用当前连接的 database。
        """
        with self._get_cursor() as cur:
            cur.execute(
            """
            SELECT 1
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = COALESCE(%s, DATABASE())
              AND TABLE_NAME = %s
            LIMIT 1
            """,
            (schema, table_name),
        )
        return cur.fetchone() is not None
    
    def ensure_table(self, create_table_sql: str, table_name: Optional[str] = None, schema: Optional[str] = None) -> None:
        """
        确保表存在：不存在则执行给定的 CREATE TABLE 语句。
        - 如果提供 table_name，会先用 information_schema 检查是否已存在；存在则不执行，避免无意义 DDL。
        - 如果不传 table_name（比如 SQL 很复杂），直接执行 CREATE TABLE IF NOT EXISTS 语句。
        """
        if table_name:
            if self.table_exists(table_name, schema=schema):
                return
        # 建议你的 create_table_sql 自带 IF NOT EXISTS，更稳
        self.execute_non_query(create_table_sql)
    
    def run_script(self, sql_text: str) -> None:
        """
        执行多条语句脚本（以分号分隔）。简单实用版：
        - 忽略空行与纯注释行（以 -- 或 # 开头）
        - 不支持存储过程/触发器里包含分号的复杂情况（那种用专门脚本执行器）
        """
        statements = []
        buff = []
        for line in sql_text.splitlines():
            l = line.strip()
            if not l or l.startswith("--") or l.startswith("#"):
                continue
            buff.append(line)
            if l.endswith(";"):
                statements.append("\n".join(buff))
                buff = []
        if buff:
            statements.append("\n".join(buff))
        for stmt in statements:
            self.execute_non_query(stmt)

    def execute_query(
        self,
        sql: str,
        params: Optional[Union[tuple, dict]] = None
    ) -> List[Dict[str, Any]]:
        """
        Execute a SELECT query and return the results.
        
        Args:
            sql: SQL query string with %s placeholders
            params: Parameters for the query as a tuple or dict
        
        Returns:
            List of dictionaries where each dictionary represents a row
            
        Example:
            results = db.execute_query(
                "SELECT * FROM users WHERE age > %s AND status = %s",
                (25, 'active')
            )
        """
        try:
            with self._get_cursor() as cursor:
                cursor.execute(sql, params or ())
                return cursor.fetchall()
        except pymysql.Error as e:
            logger.error(f"Query failed: {e}\nSQL: {sql}\nParams: {params}")
            raise
    
    def execute_non_query(
        self,
        sql: str,
        params: Optional[Union[tuple, dict]] = None
    ) -> int:
        """
        Execute an INSERT, UPDATE, or DELETE query.
        
        Args:
            sql: SQL statement with %s placeholders
            params: Parameters for the statement as a tuple or dict
            
        Returns:
            Number of rows affected
            
        Example:
            rowcount = db.execute_non_query(
                "UPDATE users SET status = %s WHERE last_login < %s",
                ('inactive', '2023-01-01')
            )
        """
        try:
            with self._get_cursor() as cursor:
                affected_rows = cursor.execute(sql, params or ())
                logger.debug(f"Query affected {affected_rows} rows")
                return affected_rows
        except pymysql.Error as e:
            logger.error(f"Query failed: {e}\nSQL: {sql}\nParams: {params}")
            raise
    
    def execute_many(
        self,
        sql: str,
        param_list: List[Union[tuple, dict]]
    ) -> int:
        """
        Execute a parameterized query multiple times.
        
        Args:
            sql: SQL statement with %s placeholders
            param_list: List of parameter tuples or dicts
            
        Returns:
            Number of rows affected (total for all executions)
            
        Example:
            users = [
                ('Alice', 'alice@example.com'),
                ('Bob', 'bob@example.com')
            ]
            rowcount = db.execute_many(
                "INSERT INTO users (name, email) VALUES (%s, %s)",
                users
            )
        """
        if not param_list:
            return 0
            
        try:
            with self._get_cursor() as cursor:
                affected_rows = cursor.executemany(sql, param_list)
                logger.debug(f"Batch query affected {affected_rows} rows")
                return affected_rows
        except pymysql.Error as e:
            logger.error(f"Batch query failed: {e}\nSQL: {sql}")
            raise
    
    def close(self) -> None:
        """Close the database connection if it's open."""
        if self.connection and self.connection.open:
            self.connection.close()
            self.connection = None
            logger.info("Database connection closed")
    
    def __enter__(self):
        """Enable usage in a context manager."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Ensure connection is closed when exiting context."""
        self.close()


# Example usage
if __name__ == "__main__":
    # Example configuration - replace with your actual database credentials
    config = {
        'host': 'localhost',
        'port': 3306,
        'user': 'your_username',
        'password': 'your_password',
        'database': 'your_database',
        'charset': 'utf8mb4',
        'cursorclass': pymysql.cursors.DictCursor
    }
    
    # Using the context manager for automatic connection handling
    with MySqlHelper(**config) as db:
        try:
            # Create a test table
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS test_users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                email VARCHAR(100) UNIQUE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            db.execute_non_query(create_table_sql)
            
            # Insert a single record
            db.execute_non_query(
                "INSERT INTO test_users (name, email) VALUES (%s, %s)",
                ('Test User', 'test@example.com')
            )
            
            # Batch insert multiple records
            users = [
                ('Alice Smith', 'alice@example.com'),
                ('Bob Johnson', 'bob@example.com'),
                ('Charlie Brown', 'charlie@example.com')
            ]
            db.execute_many(
                "INSERT IGNORE INTO test_users (name, email) VALUES (%s, %s)",
                users
            )
            
            # Query the data
            results = db.execute_query("SELECT * FROM test_users")
            print("\nAll users:")
            for row in results:
                print(f"ID: {row['id']}, Name: {row['name']}, Email: {row['email']}")
            
            # Update a record
            db.execute_non_query(
                "UPDATE test_users SET name = %s WHERE email = %s",
                ('Robert Johnson', 'bob@example.com')
            )
            
            # Query a single record
            user = db.execute_query(
                "SELECT * FROM test_users WHERE email = %s",
                ('bob@example.com',)
            )
            if user:
                print("\nUpdated user:")
                print(f"ID: {user[0]['id']}, Name: {user[0]['name']}, Email: {user[0]['email']}")
            
            # Clean up (uncomment to delete the test table when done)
            # db.execute_non_query("DROP TABLE IF EXISTS test_users")
            
        except Exception as e:
            print(f"An error occurred: {e}")
            raise

        # 改进方向：语义化包装；连接池
