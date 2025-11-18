#!/usr/bin/env python3
"""
Database connection module for market-data-pipeline.

This module provides:
- Database connection management
- Connection pooling
- Helper functions for common database operations
- Environment variable configuration
"""

import os
from typing import Optional
from contextlib import contextmanager
from dotenv import load_dotenv

try:
    import psycopg2
    from psycopg2 import pool, sql
    from psycopg2.extensions import connection, cursor
    from psycopg2.extras import RealDictCursor
except ImportError:
    raise ImportError(
        "psycopg2 is required. Install it with: pip install psycopg2-binary"
    )


# Load environment variables
load_dotenv()


class DatabaseConnection:
    """Database connection manager with pooling support."""
    
    _connection_pool: Optional[pool.ThreadedConnectionPool] = None
    _connection_string: Optional[str] = None
    
    @classmethod
    def get_connection_string(cls) -> str:
        """Get or create the database connection string."""
        if cls._connection_string is None:
            db_password = os.getenv('DB_PASSWORD')
            if not db_password:
                raise ValueError(
                    "DB_PASSWORD not found in environment variables. "
                    "Please set it in your .env file."
                )
            
            cls._connection_string = (
                f"postgresql://neondb_owner:{db_password}@"
                f"ep-wispy-mountain-af3fl2jo-pooler.c-2.us-west-2.aws.neon.tech/neondb"
                f"?sslmode=require&channel_binding=require"
            )
        
        return cls._connection_string
    
    @classmethod
    def get_connection(cls, use_pool: bool = True) -> connection:
        """
        Get a database connection.
        
        Args:
            use_pool: If True, use connection pool. If False, create a new connection.
        
        Returns:
            psycopg2 connection object
        """
        connection_string = cls.get_connection_string()
        
        if use_pool:
            if cls._connection_pool is None:
                cls._connection_pool = pool.ThreadedConnectionPool(
                    minconn=1,
                    maxconn=10,
                    dsn=connection_string
                )
            return cls._connection_pool.getconn()
        else:
            return psycopg2.connect(connection_string)
    
    @classmethod
    def return_connection(cls, conn: connection, from_pool: bool = True):
        """
        Return a connection to the pool or close it.
        
        Args:
            conn: Connection to return or close
            from_pool: If True, return to pool. If False, close the connection.
        """
        if from_pool and cls._connection_pool:
            try:
                cls._connection_pool.putconn(conn)
            except Exception:
                # If returning to pool fails, close the connection
                conn.close()
        else:
            conn.close()
    
    @classmethod
    def close_all_connections(cls):
        """Close all connections in the pool."""
        if cls._connection_pool:
            cls._connection_pool.closeall()
            cls._connection_pool = None
    
    @classmethod
    @contextmanager
    def get_cursor(cls, use_pool: bool = True, dict_cursor: bool = False):
        """
        Context manager for database cursor.
        
        Args:
            use_pool: If True, use connection pool. If False, create a new connection.
            dict_cursor: If True, use RealDictCursor (returns dict-like rows).
        
        Yields:
            psycopg2 cursor object
        
        Example:
            with DatabaseConnection.get_cursor() as cur:
                cur.execute("SELECT * FROM table;")
                results = cur.fetchall()
        """
        conn = cls.get_connection(use_pool=use_pool)
        try:
            cursor_class = RealDictCursor if dict_cursor else cursor
            cur = conn.cursor(cursor_factory=cursor_class)
            try:
                yield cur
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                cur.close()
        finally:
            cls.return_connection(conn, from_pool=use_pool)
    
    @classmethod
    @contextmanager
    def get_connection_context(cls, use_pool: bool = True):
        """
        Context manager for database connection.
        
        Args:
            use_pool: If True, use connection pool. If False, create a new connection.
        
        Yields:
            psycopg2 connection object
        
        Example:
            with DatabaseConnection.get_connection_context() as conn:
                cur = conn.cursor()
                cur.execute("SELECT * FROM table;")
                results = cur.fetchall()
                conn.commit()
        """
        conn = cls.get_connection(use_pool=use_pool)
        try:
            yield conn
        finally:
            cls.return_connection(conn, from_pool=use_pool)


# Convenience functions for common operations
def execute_query(query: str, params: Optional[tuple] = None, 
                 fetch: bool = True, use_pool: bool = True) -> Optional[list]:
    """
    Execute a query and return results.
    
    Args:
        query: SQL query string
        params: Query parameters (for parameterized queries)
        fetch: If True, fetch and return results. If False, just execute.
        use_pool: If True, use connection pool.
    
    Returns:
        List of results if fetch=True, None otherwise
    
    Example:
        results = execute_query("SELECT * FROM users WHERE id = %s", (1,))
    """
    with DatabaseConnection.get_cursor(use_pool=use_pool) as cur:
        cur.execute(query, params)
        if fetch:
            return cur.fetchall()
        return None


def execute_query_dict(query: str, params: Optional[tuple] = None,
                       use_pool: bool = True) -> list:
    """
    Execute a query and return results as dictionaries.
    
    Args:
        query: SQL query string
        params: Query parameters (for parameterized queries)
        use_pool: If True, use connection pool.
    
    Returns:
        List of dictionaries (one per row)
    
    Example:
        results = execute_query_dict("SELECT * FROM users WHERE id = %s", (1,))
        # results[0]['column_name'] to access values
    """
    with DatabaseConnection.get_cursor(use_pool=use_pool, dict_cursor=True) as cur:
        cur.execute(query, params)
        return cur.fetchall()


def execute_many(query: str, params_list: list, use_pool: bool = True):
    """
    Execute a query multiple times with different parameters.
    
    Args:
        query: SQL query string
        params_list: List of parameter tuples
        use_pool: If True, use connection pool.
    
    Example:
        execute_many(
            "INSERT INTO users (name, email) VALUES (%s, %s)",
            [('John', 'john@example.com'), ('Jane', 'jane@example.com')]
        )
    """
    with DatabaseConnection.get_cursor(use_pool=use_pool) as cur:
        cur.executemany(query, params_list)


def test_connection() -> bool:
    """
    Test database connection.
    
    Returns:
        True if connection successful, False otherwise
    """
    try:
        with DatabaseConnection.get_cursor() as cur:
            cur.execute("SELECT 1;")
            result = cur.fetchone()
            return result[0] == 1
    except Exception as e:
        print(f"Connection test failed: {e}")
        return False


def get_database_info() -> dict:
    """
    Get database information.
    
    Returns:
        Dictionary with database information
    """
    with DatabaseConnection.get_cursor(dict_cursor=True) as cur:
        cur.execute("""
            SELECT 
                current_database() as database,
                current_user as user,
                version() as version,
                NOW() as server_time,
                pg_size_pretty(pg_database_size(current_database())) as size
        """)
        return cur.fetchone()


if __name__ == '__main__':
    # Test the connection module
    print("Testing database connection module...")
    
    if test_connection():
        print("✓ Connection successful!")
        
        # Get database info
        info = get_database_info()
        print(f"\nDatabase Information:")
        print(f"  Database: {info['database']}")
        print(f"  User: {info['user']}")
        print(f"  Version: {info['version'][:50]}...")
        print(f"  Server Time: {info['server_time']}")
        print(f"  Size: {info['size']}")
        
        # Test a simple query
        results = execute_query("SELECT version();")
        if results:
            print(f"\n✓ Query execution successful")
    else:
        print("✗ Connection failed!")
    
    # Clean up
    DatabaseConnection.close_all_connections()

