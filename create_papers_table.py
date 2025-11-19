#!/usr/bin/env python3
"""
Script to create the papers table in the database if it doesn't exist.

This script:
1. Checks if the papers table exists
2. Creates the pg_trgm extension if needed (for text search)
3. Creates the papers table with all columns, indexes, and comments
4. Uses the DatabaseConnection module for database operations
"""

import sys
from db_connection import DatabaseConnection


def table_exists() -> bool:
    """Check if the papers table exists in the database."""
    with DatabaseConnection.get_cursor() as cur:
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'papers'
            );
        """)
        result = cur.fetchone()
        return result[0] if result else False


def create_pg_trgm_extension():
    """Create the pg_trgm extension if it doesn't exist."""
    with DatabaseConnection.get_cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
        print("✓ pg_trgm extension checked/created")


def create_papers_table():
    """Create the papers table with all columns."""
    create_table_sql = """
    CREATE TABLE papers (
        -- Identifiers
        id SERIAL PRIMARY KEY,
        openalex_id VARCHAR(255) UNIQUE NOT NULL,
        doi VARCHAR(500),
        
        -- Basic paper information
        title TEXT NOT NULL,
        paper_type VARCHAR(50),  -- article, etc.
        publication_date DATE,
        publication_year INTEGER,
        
        -- Primary topic/classification (flattened from nested structure)
        primary_topic_name VARCHAR(255),
        primary_topic_score DECIMAL(10, 8),
        subfield_name VARCHAR(255),  -- e.g., "Artificial Intelligence"
        field_name VARCHAR(255),     -- e.g., "Computer Science"
        domain_name VARCHAR(255),
        
        -- Open Access information (flattened)
        is_open_access BOOLEAN,
        oa_status VARCHAR(50),  -- gold, bronze, green, closed, hybrid
        
        -- Citation metrics (quantitative)
        cited_by_count INTEGER DEFAULT 0,
        citation_percentile DECIMAL(10, 8),  -- 0.0 to 1.0
        is_top_1_percent BOOLEAN,
        is_top_10_percent BOOLEAN,
        citation_percentile_min INTEGER,  -- min percentile year
        citation_percentile_max INTEGER,  -- max percentile year
        fwci DECIMAL(10, 8),  -- Field-Weighted Citation Impact
        
        -- Collaboration metrics (quantitative)
        countries_count INTEGER DEFAULT 0,
        institutions_count INTEGER DEFAULT 0,
        
        -- Metadata
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    with DatabaseConnection.get_cursor() as cur:
        cur.execute(create_table_sql)
        print("✓ Papers table created")


def create_indexes():
    """Create all indexes for the papers table."""
    indexes = [
        ("idx_papers_publication_date", "CREATE INDEX idx_papers_publication_date ON papers(publication_date);"),
        ("idx_papers_publication_year", "CREATE INDEX idx_papers_publication_year ON papers(publication_year);"),
        ("idx_papers_cited_by_count", "CREATE INDEX idx_papers_cited_by_count ON papers(cited_by_count);"),
        ("idx_papers_oa_status", "CREATE INDEX idx_papers_oa_status ON papers(oa_status);"),
        ("idx_papers_subfield", "CREATE INDEX idx_papers_subfield ON papers(subfield_name);"),
        ("idx_papers_field", "CREATE INDEX idx_papers_field ON papers(field_name);"),
        ("idx_papers_primary_topic", "CREATE INDEX idx_papers_primary_topic ON papers(primary_topic_name);"),
        ("idx_papers_citation_percentile", "CREATE INDEX idx_papers_citation_percentile ON papers(citation_percentile);"),
        ("idx_papers_fwci", "CREATE INDEX idx_papers_fwci ON papers(fwci);"),
        ("idx_papers_title_trgm", "CREATE INDEX idx_papers_title_trgm ON papers USING gin(title gin_trgm_ops);"),
    ]
    
    with DatabaseConnection.get_cursor() as cur:
        for idx_name, idx_sql in indexes:
            # Check if index already exists
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM pg_indexes 
                    WHERE schemaname = 'public' 
                    AND indexname = %s
                );
            """, (idx_name,))
            exists = cur.fetchone()[0]
            
            if exists:
                print(f"  - Index already exists: {idx_name}")
            else:
                try:
                    cur.execute(idx_sql)
                    print(f"  ✓ Created index: {idx_name}")
                except Exception as e:
                    print(f"  ✗ Failed to create index {idx_name}: {e}")
                    raise


def add_comments():
    """Add comments to the table and key columns."""
    comments = [
        ("TABLE", "papers", "Flattened schema for AI research papers tracking dashboard"),
        ("COLUMN", "papers.openalex_id", "OpenAlex unique identifier (e.g., https://openalex.org/W7105757696)"),
        ("COLUMN", "papers.citation_percentile", "Normalized citation percentile (0.0 to 1.0)"),
        ("COLUMN", "papers.fwci", "Field-Weighted Citation Impact"),
        ("COLUMN", "papers.oa_status", "Open access status: gold, bronze, green, hybrid, or closed"),
    ]
    
    with DatabaseConnection.get_cursor() as cur:
        for obj_type, obj_name, comment_text in comments:
            cur.execute(f"COMMENT ON {obj_type} {obj_name} IS %s;", (comment_text,))
            print(f"  ✓ Added comment to {obj_name}")


def main():
    """Main function to create the papers table if it doesn't exist."""
    print("Creating papers table...")
    print("=" * 50)
    
    try:
        # Check if table already exists
        if table_exists():
            print("ℹ Papers table already exists. Skipping creation.")
            return
        
        # Create pg_trgm extension (needed for text search index)
        print("\n1. Checking pg_trgm extension...")
        create_pg_trgm_extension()
        
        # Create the table
        print("\n2. Creating papers table...")
        create_papers_table()
        
        # Create indexes
        print("\n3. Creating indexes...")
        create_indexes()
        
        # Add comments
        print("\n4. Adding table and column comments...")
        add_comments()
        
        print("\n" + "=" * 50)
        print("✓ Successfully created papers table with all indexes and comments!")
        
    except Exception as e:
        print(f"\n✗ Error creating papers table: {e}")
        sys.exit(1)
    finally:
        DatabaseConnection.close_all_connections()


if __name__ == '__main__':
    main()

