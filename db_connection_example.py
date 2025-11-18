#!/usr/bin/env python3
"""
Example usage of the db_connection module.

This script demonstrates various ways to use the database connection module.
"""

from db_connection import (
    DatabaseConnection,
    execute_query,
    execute_query_dict,
    execute_many,
    test_connection,
    get_database_info
)


def example_1_context_manager():
    """Example 1: Using context manager for cursor."""
    print("\n=== Example 1: Context Manager (Cursor) ===")
    
    with DatabaseConnection.get_cursor() as cur:
        cur.execute("SELECT version();")
        result = cur.fetchone()
        print(f"PostgreSQL version: {result[0][:50]}...")


def example_2_context_manager_dict():
    """Example 2: Using context manager with dictionary cursor."""
    print("\n=== Example 2: Context Manager (Dictionary Cursor) ===")
    
    with DatabaseConnection.get_cursor(dict_cursor=True) as cur:
        cur.execute("SELECT current_user, current_database();")
        result = cur.fetchone()
        print(f"User: {result['current_user']}")
        print(f"Database: {result['current_database']}")


def example_3_connection_context():
    """Example 3: Using connection context manager."""
    print("\n=== Example 3: Connection Context Manager ===")
    
    with DatabaseConnection.get_connection_context() as conn:
        cur = conn.cursor()
        cur.execute("SELECT NOW();")
        result = cur.fetchone()
        print(f"Current time: {result[0]}")
        conn.commit()
        cur.close()


def example_4_execute_query():
    """Example 4: Using execute_query helper function."""
    print("\n=== Example 4: Execute Query Helper ===")
    
    results = execute_query("SELECT 1 + 1 as result, 'Hello' as greeting;")
    if results:
        print(f"Result: {results[0][0]}")
        print(f"Greeting: {results[0][1]}")


def example_5_execute_query_dict():
    """Example 5: Using execute_query_dict helper function."""
    print("\n=== Example 5: Execute Query Dict Helper ===")
    
    results = execute_query_dict(
        "SELECT current_user as user, current_database() as db;"
    )
    if results:
        print(f"User: {results[0]['user']}")
        print(f"Database: {results[0]['db']}")


def example_6_parameterized_query():
    """Example 6: Parameterized queries."""
    print("\n=== Example 6: Parameterized Query ===")
    
    # Safe parameterized query
    results = execute_query(
        "SELECT %s + %s as sum, %s as text;",
        (10, 20, "Parameterized query")
    )
    if results:
        print(f"Sum: {results[0][0]}")
        print(f"Text: {results[0][1]}")


def example_7_transaction():
    """Example 7: Transaction handling."""
    print("\n=== Example 7: Transaction Handling ===")
    
    with DatabaseConnection.get_connection_context() as conn:
        cur = conn.cursor()
        try:
            # Start a transaction
            cur.execute("BEGIN;")
            
            # Execute some queries
            cur.execute("SELECT 1;")
            result1 = cur.fetchone()
            
            cur.execute("SELECT 2;")
            result2 = cur.fetchone()
            
            # Commit the transaction
            conn.commit()
            print(f"Transaction committed. Results: {result1[0]}, {result2[0]}")
        except Exception as e:
            # Rollback on error
            conn.rollback()
            print(f"Transaction rolled back due to error: {e}")
        finally:
            cur.close()


def example_8_no_pool():
    """Example 8: Creating a connection without pooling."""
    print("\n=== Example 8: Connection Without Pool ===")
    
    # Use a direct connection (not from pool)
    with DatabaseConnection.get_cursor(use_pool=False) as cur:
        cur.execute("SELECT current_setting('server_version');")
        version = cur.fetchone()[0]
        print(f"Server version: {version}")


def main():
    """Run all examples."""
    print("=" * 60)
    print("Database Connection Module - Usage Examples")
    print("=" * 60)
    
    # Test connection first
    if not test_connection():
        print("ERROR: Database connection failed!")
        return
    
    # Get database info
    info = get_database_info()
    print(f"\nConnected to: {info['database']} as {info['user']}")
    
    # Run examples
    try:
        example_1_context_manager()
        example_2_context_manager_dict()
        example_3_connection_context()
        example_4_execute_query()
        example_5_execute_query_dict()
        example_6_parameterized_query()
        example_7_transaction()
        example_8_no_pool()
        
        print("\n" + "=" * 60)
        print("All examples completed successfully!")
        print("=" * 60)
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Clean up connections
        DatabaseConnection.close_all_connections()


if __name__ == '__main__':
    main()

